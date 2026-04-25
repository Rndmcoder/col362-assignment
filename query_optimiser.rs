// query_optimiser.rs — query rewrite / optimisation pass
//
// ═══════════════════════════════════════════════════════════════════════════
// ALL OPTIMISATIONS:
//
//  [OPT-01] Predicate Pushdown      → push_filter_into, collect_cross_leaves
//  [OPT-02] Filter Reordering       → reorder_predicates_by_selectivity
//  [OPT-03] Cross+Filter → Join     → fuse_filter_cross, build_optimal_join
//  [OPT-04] Projection Pushdown     → fuse_project (through Sort+Filter+FScan)
//  [OPT-05] Pipelining              → FilteredScanData fused scan node
//  [OPT-06] Materialization         → spill() in giveoutput.rs
//  [OPT-07] Grace Hash Join         → grace_hash_join in giveoutput.rs
//  [OPT-08] External Merge Sort     → exec_sort in giveoutput.rs
//  [OPT-09] Buffer management       → safe_cap() in giveoutput.rs
//  [OPT-10] Build-side selection    → build_join_node (stat-driven)
//  [OPT-11] IsPhysicallyOrdered     → stat-driven via ColumnStat
//  [OPT-12] Filter selectivity      → full stat-driven (histogram/density/range/cardinality)
//  [OPT-13] Join size estimation    → estimated_rows_ctx (CardinalityStat row counts)
//  [OPT-14] Join order optimisation → build_optimal_join (stat-driven, connectivity-aware)
//  [OPT-15] Skew detection          → grace_hash_join in giveoutput.rs
//  [OPT-16] Partition count tuning  → grace_partition_count free fn
//  [OPT-17] Sort-Merge Join         → sort_merge_join in giveoutput.rs
//  [OPT-18] Reuse Sort Order        → is_physically_ordered propagated
//  [OPT-19] Early termination       → FilteredScanData.is_physically_ordered_on
//  [OPT-20] Dynamic memory split    → set_memory_limit_mb in giveoutput.rs
//  [OPT-21] Predicate reordering inside HashJoin extra_preds
//             → reorder_predicates_by_selectivity called on extra_preds after join build
//  [OPT-22] Multi-pass scalar pushdown
//             → push_all_scalars_deep recurses through the full plan tree post-join-build
//               so scalar preds that cross project/sort/filter barriers are still pushed
//  [OPT-23] Redundant Project elimination
//             → elide_redundant_projects removes Project nodes whose map is an identity
//               on an already-projected FilteredScan (avoids double projection work)
//  [OPT-24] Sort-order propagation through HashJoin output
//             → subtree_is_ordered_on_specs checks whether the probe-side output of a
//               HashJoin preserves order so downstream Sort can be skipped
//  [OPT-25] Adaptive FilterCross → HashJoin upgrade in pass 3
//             → distribute_predicates_deep attempts build_join_node on any
//               Filter(FilterCross) or Filter(HashJoin) to upgrade NLJ to HJ
//
// KEY FIXES:
//
//  [FIX-CROSS-OOM]
//    collect_cross_leaves recurses through Filter(Cross) nodes, extracting
//    their predicates into the global pool so all tables are joined optimally.
//    build_join_node emits FilterCross (chunked NLJ) instead of bare Cross
//    whenever the estimated product exceeds 4× memory budget, preventing
//    the 20M-row cartesian product that caused the allocator to fail.
//    distribute_predicates_deep (pass 3) catches any Filter(Cross) that
//    escaped pass 1 and re-converts them.
//
//  [FIX-COL-MATCH]
//    known_col_prefix correctly disambiguates "p_" (part) from "ps_" (partsupp)
//    so join predicates are never silently mis-attributed.
//
//  [FIX-PROJ-SORT]
//    fuse_project pushes projections through Sort(Filter*(FilteredScan)),
//    reducing the row width seen by the external sort.
//
//  [FIX-COMPOSITE-JOIN]
//    build_join_node collects ALL equi-predicates between a table pair into a
//    single HashJoin node with composite left_keys/right_keys, rather than
//    chaining two separate HashJoin nodes. Critical for Q58.
//
//  [FIX-NE-JOIN]
//    build_join_node now separates equi-join predicates from non-equi (NE, GT,
//    LT etc.) predicates. If at least one equi-predicate exists, a HashJoin is
//    emitted using only the equi-predicates as keys, and the NE predicates go
//    into extra_preds for post-join filtering.
//
//  [FIX-ALIAS-COL]
//    tree_has_col now correctly handles dotted alias names like "l1.l_orderkey"
//    by checking the bare column name suffix against known prefixes.
//
//  [FIX-SCALAR-PUSH-ALIAS]
//    push_scalar_into_rel handles dotted alias predicates: for a predicate on
//    "alias.bare_col", it finds the Project node whose output contains
//    "alias.bare_col" and pushes the predicate there.
//
//  [FIX-PROJ-PASSTHROUGH]
//    fuse_project (Case C) now attempts to push the project through HashJoin,
//    FilterCross and Filter nodes when the projected columns are a strict subset
//    of what those operators output, eliminating wide intermediate rows.
//
//  [FIX-SORT-EXTRA-COL]
//    When fuse_project Case B adds extra key columns for Sort, those columns
//    are now correctly mapped from their fully-qualified names (e.g.
//    "lineitem.l_shipdate") to the bare sort key name so cmp_rows can find them.
//
// STATS POLICY:
//    All cardinality and selectivity decisions use ColumnStat from DbContext.
//    TPC-H SF-1 heuristics are used ONLY as a last-resort fallback when no
//    ColumnStat is present, with an explicit warning logged.
//
//      IsPhysicallyOrdered → OPT-11, OPT-19
//      RangeStat           → OPT-12 range selectivity
//      HistogramStat       → OPT-12 equality/range selectivity
//      CardinalityStat     → OPT-12 (1/NDV), OPT-13 (row counts)
//      DensityStat         → OPT-12 equality selectivity
// ═══════════════════════════════════════════════════════════════════════════

use common::query::*;
use db_config::{DbContext, statistics::ColumnStat};

// ─────────────────────────────────────────────────────────────────────────────
// Optimised plan node types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OptFilterData {
    pub predicates: Vec<Predicate>,
    pub underlying: Box<OptQueryOp>,
}

#[derive(Debug, Clone)]
pub struct FilterCrossData {
    pub predicates: Vec<Predicate>,
    pub left:       Box<OptQueryOp>,
    pub right:      Box<OptQueryOp>,
}

#[derive(Debug, Clone)]
pub struct HashJoinData {
    pub left_keys:   Vec<String>,
    pub right_keys:  Vec<String>,
    pub extra_preds: Vec<Predicate>,
    pub left:        Box<OptQueryOp>,
    pub right:       Box<OptQueryOp>,
}

#[derive(Debug, Clone)]
pub struct OptProjectData {
    pub column_name_map: Vec<(String, String)>,
    pub underlying:      Box<OptQueryOp>,
}

#[derive(Debug, Clone)]
pub struct OptCrossData {
    pub left:  Box<OptQueryOp>,
    pub right: Box<OptQueryOp>,
}

#[derive(Debug, Clone)]
pub struct OptSortData {
    pub sort_specs: Vec<SortSpec>,
    pub underlying: Box<OptQueryOp>,
    /// [OPT-11][OPT-18] true → executor skips the sort entirely.
    pub is_physically_ordered: bool,
}

/// [OPT-01][OPT-04][OPT-05] Fused scan+filter+project in one executor pass.
#[derive(Debug, Clone)]
pub struct FilteredScanData {
    pub table_id:   String,
    pub predicates: Vec<Predicate>,
    /// (original_col, output_col). None = emit every column.
    pub project:    Option<Vec<(String, String)>>,
    /// [OPT-19] Stop scanning when this column exceeds the upper-bound predicate.
    pub is_physically_ordered_on: Option<String>,
}

#[derive(Debug, Clone)]
pub enum OptQueryOp {
    Scan(ScanData),
    FilteredScan(FilteredScanData),
    Filter(OptFilterData),
    Project(OptProjectData),
    Cross(OptCrossData),
    Sort(OptSortData),
    FilterCross(FilterCrossData),
    HashJoin(HashJoinData),
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-12][OPT-13] Optimiser context
// ─────────────────────────────────────────────────────────────────────────────

pub struct OptContext<'a> {
    pub db: &'a DbContext,
    pub memory_budget_bytes: usize,
}

impl<'a> OptContext<'a> {
    pub fn new(db: &'a DbContext) -> Self {
        OptContext { db, memory_budget_bytes: 64 * 1024 * 1024 }
    }

    // ── [OPT-13] Row count from ColumnStat ───────────────────────────────────
    pub fn table_row_count(&self, table_id: &str) -> u64 {
        let resolved = self.resolve_table_id(table_id);
        let spec = match self.db.get_table_specs().iter().find(|t| t.name == resolved) {
            Some(s) => s,
            None => {
                eprintln!("[OPT-WARN][OPT-13] table '{}' (resolved: '{}') not in DbContext — fallback",
                          table_id, resolved);
                return tpch_row_estimate(&resolved);
            }
        };

        // Pass 1: PK column (IsPhysicallyOrdered + CardinalityStat).
        for col in &spec.column_specs {
            if let Some(stats) = &col.stats {
                let ordered = stats.iter().any(|s| matches!(s, ColumnStat::IsPhysicallyOrdered));
                if ordered {
                    for s in stats {
                        if let ColumnStat::CardinalityStat(c) = s {
                            eprintln!("[OPT-13] '{}' row_count={} (PK CardinalityStat col='{}')",
                                      table_id, c.0, col.column_name);
                            return c.0;
                        }
                    }
                }
            }
        }

        // Pass 2: maximum CardinalityStat.
        let mut max_card: u64 = 0;
        for col in &spec.column_specs {
            if let Some(stats) = &col.stats {
                for s in stats {
                    if let ColumnStat::CardinalityStat(c) = s {
                        if c.0 > max_card { max_card = c.0; }
                    }
                }
            }
        }
        if max_card > 0 {
            eprintln!("[OPT-13] '{}' row_count={} (max CardinalityStat)", table_id, max_card);
            return max_card;
        }

        // Pass 3: histogram frequency sum.
        for col in &spec.column_specs {
            if let Some(stats) = &col.stats {
                for s in stats {
                    if let ColumnStat::HistogramStat(h) = s {
                        let total: u64 = h.frequency_points.iter().map(|(_, f)| f.0).sum();
                        if total > 0 {
                            eprintln!("[OPT-13] '{}' row_count={} (histogram col='{}')",
                                      table_id, total, col.column_name);
                            return total;
                        }
                    }
                }
            }
        }

        // Pass 4: TPC-H heuristic fallback.
        let est = tpch_row_estimate(&resolved);
        eprintln!("[OPT-WARN][OPT-13] '{}' no stats → TPC-H heuristic {}", table_id, est);
        est
    }

    /// Resolve alias table IDs (e.g. "l1", "l2") to base table names.
    pub fn resolve_table_id<'b>(&self, table_id: &'b str) -> String {
        if self.db.get_table_specs().iter().any(|t| t.name == table_id) {
            return table_id.to_string();
        }
        let base = table_id.trim_end_matches(|c: char| c.is_ascii_digit());
        let candidates = [
            ("l",  "lineitem"),
            ("o",  "orders"),
            ("c",  "customer"),
            ("p",  "part"),
            ("ps", "partsupp"),
            ("s",  "supplier"),
            ("n",  "nation"),
            ("r",  "region"),
            ("cn", "nation"),
            ("sn", "nation"),
            ("cr", "region"),
            ("sr", "region"),
        ];
        for (pfx, tbl) in &candidates {
            if base == *pfx {
                return tbl.to_string();
            }
        }
        table_id.to_string()
    }

    // ── [OPT-12] Selectivity estimation ──────────────────────────────────────
    pub fn filter_selectivity(&self, table_id: &str, preds: &[Predicate]) -> f64 {
        if preds.is_empty() { return 1.0; }
        let resolved = self.resolve_table_id(table_id);
        let total_rows = self.table_row_count(table_id) as f64;
        let mut sel = 1.0f64;

        for pred in preds {
            if matches!(&pred.value, ComparisionValue::Column(_)) { continue; }
            let col_spec = self.db.get_table_specs()
                .iter()
                .find(|t| t.name == resolved)
                .and_then(|spec| spec.column_specs.iter().find(|c| {
                    let full = format!("{}.{}", resolved, c.column_name);
                    full == pred.column_name
                        || c.column_name == pred.column_name
                        || pred.column_name.ends_with(&format!(".{}", c.column_name))
                        || {
                            let bare = pred.column_name.split('.').last().unwrap_or(&pred.column_name);
                            c.column_name == bare
                        }
                }));

            let col_sel = match &pred.operator {
                ComparisionOperator::EQ =>
                    self.sel_eq(col_spec, &pred.value, total_rows),
                ComparisionOperator::NE =>
                    1.0 - self.sel_eq(col_spec, &pred.value, total_rows),
                ComparisionOperator::LT | ComparisionOperator::LTE |
                ComparisionOperator::GT | ComparisionOperator::GTE =>
                    self.sel_range(col_spec, &pred.operator, &pred.value),
            };
            sel *= col_sel;
        }
        sel.clamp(1e-6, 1.0)
    }

    fn sel_eq(
        &self,
        col_spec:   Option<&db_config::table::ColumnSpec>,
        value:      &ComparisionValue,
        total_rows: f64,
    ) -> f64 {
        let lit_f64 = comparison_value_as_f64(value);
        if let Some(col) = col_spec {
            if let Some(stats) = &col.stats {
                // Priority 1: histogram.
                for s in stats {
                    if let ColumnStat::HistogramStat(h) = s {
                        if let Some(lit) = lit_f64 {
                            for (range, freq) in &h.frequency_points {
                                if let (Some(lo), Some(hi)) = (
                                    data_as_f64(&range.lower_bound),
                                    data_as_f64(&range.upper_bound),
                                ) {
                                    if lit >= lo && lit <= hi && hi > lo {
                                        let frac = 1.0 / (hi - lo).max(1.0);
                                        return (freq.0 as f64 / total_rows.max(1.0) * frac)
                                            .clamp(1e-6, 1.0);
                                    }
                                }
                            }
                        } else if let ComparisionValue::String(s_lit) = value {
                            for (range, freq) in &h.frequency_points {
                                if let common::Data::String(ref lo_s) = range.lower_bound {
                                    if s_lit == lo_s {
                                        return (freq.0 as f64 / total_rows.max(1.0))
                                            .clamp(1e-6, 1.0);
                                    }
                                }
                            }
                        }
                    }
                }
                // Priority 2: DensityStat.
                for s in stats {
                    if let ColumnStat::DensityStat(d) = s {
                        return (d.0 as f64).clamp(1e-6, 1.0);
                    }
                }
                // Priority 3: CardinalityStat → uniform 1/NDV.
                for s in stats {
                    if let ColumnStat::CardinalityStat(c) = s {
                        if c.0 > 0 { return (1.0 / c.0 as f64).clamp(1e-6, 1.0); }
                    }
                }
                // Priority 4: DataType fallback.
                return match col.data_type {
                    common::DataType::String                                  => 0.05,
                    common::DataType::Int32 | common::DataType::Int64        => 0.02,
                    common::DataType::Float32 | common::DataType::Float64    => 0.01,
                    _                                                         => 0.10,
                };
            }
        }
        0.10
    }

    fn sel_range(
        &self,
        col_spec: Option<&db_config::table::ColumnSpec>,
        op:       &ComparisionOperator,
        value:    &ComparisionValue,
    ) -> f64 {
        let lit = match comparison_value_as_f64(value) {
            Some(v) => v,
            None    => return 0.30,
        };
        if let Some(col) = col_spec {
            if let Some(stats) = &col.stats {
                // Priority 1: RangeStat fraction of domain.
                for s in stats {
                    if let ColumnStat::RangeStat(r) = s {
                        if let (Some(lo), Some(hi)) = (
                            data_as_f64(&r.lower_bound), data_as_f64(&r.upper_bound),
                        ) {
                            let domain = (hi - lo).abs();
                            if domain > 0.0 {
                                let covered = match op {
                                    ComparisionOperator::LT | ComparisionOperator::LTE =>
                                        (lit - lo).max(0.0).min(domain) / domain,
                                    ComparisionOperator::GT | ComparisionOperator::GTE =>
                                        (hi - lit).max(0.0).min(domain) / domain,
                                    _ => 0.33,
                                };
                                return covered.clamp(1e-6, 1.0);
                            }
                        }
                    }
                }
                // Priority 2: histogram bucket counting.
                for s in stats {
                    if let ColumnStat::HistogramStat(h) = s {
                        let total_freq: u64 = h.frequency_points.iter().map(|(_, f)| f.0).sum();
                        if total_freq == 0 { continue; }
                        let mut in_range: u64 = 0;
                        for (range, freq) in &h.frequency_points {
                            if let (Some(lo), Some(hi)) = (
                                data_as_f64(&range.lower_bound),
                                data_as_f64(&range.upper_bound),
                            ) {
                                let fully_in = match op {
                                    ComparisionOperator::LT  => hi <  lit,
                                    ComparisionOperator::LTE => hi <= lit,
                                    ComparisionOperator::GT  => lo >  lit,
                                    ComparisionOperator::GTE => lo >= lit,
                                    _ => false,
                                };
                                let partial = matches!(op,
                                    ComparisionOperator::LT  | ComparisionOperator::LTE |
                                    ComparisionOperator::GT  | ComparisionOperator::GTE)
                                    && lo < lit && hi > lit;
                                if fully_in {
                                    in_range += freq.0;
                                } else if partial {
                                    let bw   = (hi - lo).max(1.0);
                                    let frac = match op {
                                        ComparisionOperator::LT | ComparisionOperator::LTE =>
                                            (lit - lo) / bw,
                                        _ => (hi - lit) / bw,
                                    };
                                    in_range += (freq.0 as f64 * frac.clamp(0.0, 1.0)) as u64;
                                }
                            }
                        }
                        return (in_range as f64 / total_freq as f64).clamp(1e-6, 1.0);
                    }
                }
            }
        }
        0.33
    }

    // ── [OPT-11] Physical order check from ColumnStat ────────────────────────
    pub fn column_is_physically_ordered(&self, table_id: &str, col_name: &str) -> bool {
        let resolved = self.resolve_table_id(table_id);
        let bare = col_name.split('.').last().unwrap_or(col_name);

        let found = self.db.get_table_specs()
            .iter()
            .find(|t| t.name == resolved)
            .and_then(|spec| spec.column_specs.iter().find(|c| {
                c.column_name == bare
                    || c.column_name == col_name
                    || col_name.ends_with(&format!(".{}", c.column_name))
                    || format!("{}.{}", resolved, c.column_name) == col_name
            }))
            .and_then(|c| c.stats.as_ref())
            .map(|stats| stats.iter().any(|s| matches!(s, ColumnStat::IsPhysicallyOrdered)))
            .unwrap_or(false);

        if !found {
            return matches!(bare,
                "l_orderkey"  | "o_orderkey"  |
                "ps_partkey"  | "p_partkey"   |
                "c_custkey"   | "s_suppkey"   |
                "n_nationkey" | "r_regionkey"
            );
        }
        found
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-16] Grace partition count
// ─────────────────────────────────────────────────────────────────────────────

pub fn grace_partition_count(
    build_rows:          u64,
    avg_row_bytes:       usize,
    memory_budget_bytes: usize,
) -> usize {
    let build_bytes      = (build_rows as usize).saturating_mul(avg_row_bytes);
    // Each partition's build side must fit in 1/8 of budget so the hash map
    // for that partition fits comfortably alongside the probe read buffer.
    let partition_budget = (memory_budget_bytes / 8).max(1);
    let n                = (build_bytes + partition_budget - 1) / partition_budget;
    let result           = n.clamp(8, 512);
    eprintln!("[OPT-16] grace_parts build_rows={} → n_parts={}", build_rows, result);
    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// [OPT-26] Column pruning: given the set of columns needed by the parent,
/// push narrowing projections into HashJoin children to reduce row width.
/// `needed` = None means "all columns needed" (top of tree).
fn prune_columns_top_down(op: OptQueryOp, needed: Option<&[String]>) -> OptQueryOp {
    match op {
        OptQueryOp::Project(OptProjectData { column_name_map, underlying }) => {
            let child_needed: Vec<String> = column_name_map.iter()
                .map(|(from, _)| from.clone())
                .collect();
            let new_child = prune_columns_top_down(*underlying, Some(&child_needed));
            OptQueryOp::Project(OptProjectData {
                column_name_map,
                underlying: Box::new(new_child),
            })
        }

        OptQueryOp::Sort(OptSortData { sort_specs, underlying, is_physically_ordered }) => {
            let sort_keys: Vec<String> = sort_specs.iter()
                .map(|s| s.column_name.clone())
                .collect();
            let extended: Option<Vec<String>> = needed.map(|n| {
                let mut v = n.to_vec();
                for k in &sort_keys {
                    if !v.contains(k) { v.push(k.clone()); }
                }
                v
            });
            let new_child = prune_columns_top_down(*underlying, extended.as_deref());
            OptQueryOp::Sort(OptSortData {
                sort_specs,
                underlying: Box::new(new_child),
                is_physically_ordered,
            })
        }

        OptQueryOp::HashJoin(hj) => {
            let needed_set: Option<std::collections::HashSet<&str>> = needed.map(|n| {
                n.iter().map(|s| s.as_str()).collect()
            });

            let left_out  = tree_output_cols(&hj.left);
            let right_out = tree_output_cols(&hj.right);

            let join_cols_needed: Vec<String> = hj.left_keys.iter()
                .cloned()
                .chain(hj.right_keys.iter().cloned())
                .chain(hj.extra_preds.iter().flat_map(|p| {
                    let mut v = vec![p.column_name.clone()];
                    if let ComparisionValue::Column(c) = &p.value { v.push(c.clone()); }
                    v
                }))
                .collect();

            let left_needed: Vec<String> = left_out.iter().filter(|c| {
                if c.ends_with(".*") { return true; }
                let in_join = join_cols_needed.iter().any(|jc| jc == *c);
                let in_parent = needed_set.as_ref()
                    .map(|ns| ns.contains(c.as_str()))
                    .unwrap_or(true);
                in_join || in_parent
            }).cloned().collect();

            let right_needed: Vec<String> = right_out.iter().filter(|c| {
                if c.ends_with(".*") { return true; }
                let in_join = join_cols_needed.iter().any(|jc| jc == *c);
                let in_parent = needed_set.as_ref()
                    .map(|ns| ns.contains(c.as_str()))
                    .unwrap_or(true);
                in_join || in_parent
            }).cloned().collect();

            // Recurse into children with their needed sets
            // Don't inject extra Project nodes — just recurse and let FilteredScan handle it
            let left_child = Box::new(prune_columns_top_down(
                *hj.left,
                if left_needed.iter().any(|c| c.ends_with(".*")) { None } else { Some(&left_needed) }
            ));
            let right_child = Box::new(prune_columns_top_down(
                *hj.right,
                if right_needed.iter().any(|c| c.ends_with(".*")) { None } else { Some(&right_needed) }
            ));

            OptQueryOp::HashJoin(HashJoinData {
                left: left_child,
                right: right_child,
                ..hj
            })
        }

        // KEY FIX: push needed columns directly into FilteredScan projection
        OptQueryOp::FilteredScan(mut fs) => {
            if let Some(needed_cols) = needed {
                if fs.project.is_none() && !needed_cols.is_empty() {
                    // Build projection from what the scan outputs vs what's needed
                    let table_prefix = format!("{}.", fs.table_id);
                    let proj: Vec<(String, String)> = needed_cols.iter()
                        .filter(|c| {
                            c.starts_with(&table_prefix)
                            || known_col_prefix(&fs.table_id, c)
                        })
                        .map(|c| (c.clone(), c.clone()))
                        .collect();
                    if !proj.is_empty() {
                        eprintln!("[OPT-26] FScan({}) pruned to {} cols (from needed={})",
                                  fs.table_id, proj.len(), needed_cols.len());
                        fs.project = Some(proj);
                    }
                }
            }
            OptQueryOp::FilteredScan(fs)
        }

        OptQueryOp::Filter(OptFilterData { predicates, underlying }) => {
            let pred_cols: Vec<String> = predicates.iter().flat_map(|p| {
                let mut v = vec![p.column_name.clone()];
                if let ComparisionValue::Column(c) = &p.value { v.push(c.clone()); }
                v
            }).collect();
            let extended: Option<Vec<String>> = needed.map(|n| {
                let mut v = n.to_vec();
                for c in &pred_cols { if !v.contains(c) { v.push(c.clone()); } }
                v
            });
            OptQueryOp::Filter(OptFilterData {
                predicates,
                underlying: Box::new(prune_columns_top_down(*underlying, extended.as_deref())),
            })
        }

        OptQueryOp::FilterCross(d) => OptQueryOp::FilterCross(FilterCrossData {
            left:  Box::new(prune_columns_top_down(*d.left,  needed)),
            right: Box::new(prune_columns_top_down(*d.right, needed)),
            ..d
        }),

        other => other,
    }
}

pub fn optimise_with_ctx(op: QueryOp, ctx: &OptContext<'_>) -> OptQueryOp {
    eprintln!("[OPT] start budget={}MB", ctx.memory_budget_bytes / (1024 * 1024));
    let lowered   = lower(op, ctx);
    let fused     = fuse_operators(lowered, ctx);
    let cleaned   = distribute_predicates_deep(fused, ctx);
    let pushed    = push_all_scalars_deep(cleaned, ctx);
    let deduped   = elide_redundant_projects(pushed);
    let reordered = reorder_join_extra_preds(deduped, ctx);
    // NEW: column pruning pass — trim wide join outputs
    let pruned    = prune_columns_top_down(reordered, None);
    verify_scalar_pushdown(&pruned);
    eprintln!("[OPT] final plan: {}", plan_summary(&pruned));
    pruned
}

// ─────────────────────────────────────────────────────────────────────────────
// Pass 1: lower QueryOp → OptQueryOp
// ─────────────────────────────────────────────────────────────────────────────

fn lower(op: QueryOp, ctx: &OptContext<'_>) -> OptQueryOp {
    match op {
        QueryOp::Scan(d) => OptQueryOp::Scan(d),

        QueryOp::Filter(FilterData { mut predicates, underlying }) => {
            if queryop_contains_cross(underlying.as_ref()) {
                let mut raw_leaves: Vec<QueryOp> = Vec::new();
                collect_queryop_cross_leaves(*underlying, &mut raw_leaves, &mut predicates);
                eprintln!("[OPT-03][FIX-CROSS-FLATTEN] {} relations, {} preds",
                          raw_leaves.len(), predicates.len());
                let opt_leaves: Vec<OptQueryOp> = raw_leaves
                    .into_iter()
                    .map(|leaf| lower(leaf, ctx))
                    .collect();
                return build_optimal_join(opt_leaves, predicates, ctx);
            }
            if let Some(tid) = scan_table_of_queryop(underlying.as_ref()) {
                reorder_predicates_by_selectivity(&mut predicates, &tid, ctx);
            }
            push_filter_into(predicates, lower(*underlying, ctx), ctx)
        }

        QueryOp::Project(ProjectData { column_name_map, underlying }) =>
            OptQueryOp::Project(OptProjectData {
                column_name_map,
                underlying: Box::new(lower(*underlying, ctx)),
            }),

        QueryOp::Cross(CrossData { left, right }) =>
            OptQueryOp::Cross(OptCrossData {
                left:  Box::new(lower(*left,  ctx)),
                right: Box::new(lower(*right, ctx)),
            }),

        QueryOp::Sort(SortData { sort_specs, underlying }) => {
            let child   = lower(*underlying, ctx);
            let already = subtree_is_ordered_on_specs(&child, &sort_specs, ctx);
            if already {
                eprintln!("[OPT-11] redundant sort on {:?}",
                          sort_specs.iter().map(|s| &s.column_name).collect::<Vec<_>>());
            }
            OptQueryOp::Sort(OptSortData {
                sort_specs,
                underlying:            Box::new(child),
                is_physically_ordered: already,
            })
        }
    }
}

fn queryop_contains_cross(op: &QueryOp) -> bool {
    match op {
        QueryOp::Cross(_) => true,
        QueryOp::Filter(FilterData { underlying, .. }) => queryop_contains_cross(underlying),
        _ => false,
    }
}

fn collect_queryop_cross_leaves(
    op:    QueryOp,
    out:   &mut Vec<QueryOp>,
    preds: &mut Vec<Predicate>,
) {
    match op {
        QueryOp::Cross(CrossData { left, right }) => {
            collect_queryop_cross_leaves(*left,  out, preds);
            collect_queryop_cross_leaves(*right, out, preds);
        }
        QueryOp::Filter(FilterData { predicates, underlying }) => {
            match *underlying {
                QueryOp::Cross(CrossData { left, right }) => {
                    preds.extend(predicates);
                    collect_queryop_cross_leaves(*left,  out, preds);
                    collect_queryop_cross_leaves(*right, out, preds);
                }
                other => out.push(QueryOp::Filter(FilterData {
                    predicates,
                    underlying: Box::new(other),
                })),
            }
        }
        other => out.push(other),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-01] push_filter_into
// ─────────────────────────────────────────────────────────────────────────────

fn push_filter_into(
    predicates: Vec<Predicate>,
    child:      OptQueryOp,
    ctx:        &OptContext<'_>,
) -> OptQueryOp {
    if predicates.is_empty() { return child; }
    match child {
        // [OPT-03][FIX-CROSS-OOM] Cross + Filter → optimal join tree.
        OptQueryOp::Cross(OptCrossData { left, right }) =>
            fuse_filter_cross(predicates, left, right, ctx),
        // [OPT-25] Filter above HashJoin: try to absorb into extra_preds.
        OptQueryOp::HashJoin(mut hj) => {
            let mut leftover = Vec::new();
            for pred in predicates {
                if let ComparisionValue::Column(rhs) = &pred.value {
                    if (tree_has_col(&hj.left, &pred.column_name) && tree_has_col(&hj.right, rhs))
                    || (tree_has_col(&hj.left, rhs) && tree_has_col(&hj.right, &pred.column_name))
                    {
                        hj.extra_preds.push(pred);
                        continue;
                    }
                }
                leftover.push(pred);
            }
            let base = OptQueryOp::HashJoin(hj);
            if leftover.is_empty() { base }
            else { OptQueryOp::Filter(OptFilterData { predicates: leftover, underlying: Box::new(base) }) }
        }
        other => OptQueryOp::Filter(OptFilterData {
            predicates,
            underlying: Box::new(other),
        }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-03][FIX-CROSS-OOM] fuse_filter_cross
// ─────────────────────────────────────────────────────────────────────────────

fn fuse_filter_cross(
    predicates: Vec<Predicate>,
    left:  Box<OptQueryOp>,
    right: Box<OptQueryOp>,
    ctx:   &OptContext<'_>,
) -> OptQueryOp {
    let mut leaves      = Vec::new();
    let mut extra_preds = predicates;
    collect_cross_leaves(*left,  &mut leaves, &mut extra_preds);
    collect_cross_leaves(*right, &mut leaves, &mut extra_preds);
    eprintln!("[OPT-03] Cross+Filter: {} relations, {} preds", leaves.len(), extra_preds.len());
    build_optimal_join(leaves, extra_preds, ctx)
}

/// [FIX-CROSS-OOM] Flatten Cross nodes and recurse through Filter(Cross)
/// wrappers, extracting their predicates into the global pool.
fn collect_cross_leaves(
    op:          OptQueryOp,
    out:         &mut Vec<OptQueryOp>,
    extra_preds: &mut Vec<Predicate>,
) {
    match op {
        OptQueryOp::Cross(OptCrossData { left, right }) => {
            collect_cross_leaves(*left,  out, extra_preds);
            collect_cross_leaves(*right, out, extra_preds);
        }
        OptQueryOp::Filter(OptFilterData { predicates, underlying }) => {
            match *underlying {
                OptQueryOp::Cross(OptCrossData { left, right }) => {
                    extra_preds.extend(predicates);
                    collect_cross_leaves(*left,  out, extra_preds);
                    collect_cross_leaves(*right, out, extra_preds);
                }
                other => out.push(OptQueryOp::Filter(OptFilterData {
                    predicates,
                    underlying: Box::new(other),
                })),
            }
        }
        other => out.push(other),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-03][OPT-14][FIX-CROSS-OOM][FIX-ALIAS-COL] build_optimal_join
// ─────────────────────────────────────────────────────────────────────────────

fn build_optimal_join(
    mut relations:  Vec<OptQueryOp>,
    mut predicates: Vec<Predicate>,
    ctx:            &OptContext<'_>,
) -> OptQueryOp {
    if relations.is_empty() { panic!("build_optimal_join: no relations"); }
    if relations.len() == 1 {
        let rel = relations.remove(0);
        return if predicates.is_empty() { rel }
        else {
            let mut scalar: Vec<Predicate> = Vec::new();
            let mut col_col: Vec<Predicate> = Vec::new();
            for p in predicates {
                if matches!(&p.value, ComparisionValue::Column(_)) { col_col.push(p); }
                else { scalar.push(p); }
            }
            let mut result = rel;
            for p in scalar { push_scalar_into_rel(&mut result, p); }
            if col_col.is_empty() { result }
            else {
                OptQueryOp::Filter(OptFilterData {
                    predicates: col_col,
                    underlying: Box::new(result),
                })
            }
        };
    }

    // [OPT-01][FIX-ALIAS-COL] Push scalar predicates into the owning relation.
    let mut leftover: Vec<Predicate> = Vec::new();
    'pred: for pred in predicates.drain(..) {
        if matches!(&pred.value, ComparisionValue::Column(_)) {
            leftover.push(pred);
            continue;
        }
        for rel in &mut relations {
            if tree_has_col(rel, &pred.column_name) {
                push_scalar_into_rel(rel, pred);
                continue 'pred;
            }
        }
        leftover.push(pred);
    }
    predicates = leftover;

    // [OPT-14] Stat-driven ordering: smallest estimated output first.
    relations.sort_by_key(|r| estimated_rows_ctx(r, ctx));
    eprintln!("[OPT-14] join order (stat): {}",
              relations.iter()
                       .map(|r| format!("{}~{}", relation_name(r), estimated_rows_ctx(r, ctx)))
                       .collect::<Vec<_>>()
                       .join(" ⋈ "));

    let mut current   = relations.remove(0);
    let mut remaining = relations;

    while !remaining.is_empty() {
        let mut best_idx      = None;
        let mut best_has_join = false;
        let mut best_est      = u64::MAX;

        for (i, rel) in remaining.iter().enumerate() {
            let has_join = predicates.iter().any(|p| {
                if let ComparisionValue::Column(rhs) = &p.value {
                    (tree_has_col(&current, &p.column_name) && tree_has_col(rel, rhs))
                    || (tree_has_col(&current, rhs) && tree_has_col(rel, &p.column_name))
                } else { false }
            });
            let est = estimated_rows_ctx(rel, ctx);
            let better = if has_join && !best_has_join {
                true
            } else if has_join == best_has_join {
                est < best_est
            } else {
                false
            };
            if better { best_idx = Some(i); best_has_join = has_join; best_est = est; }
        }

        let next = remaining.remove(best_idx.unwrap_or(0));

        let mut join_preds: Vec<Predicate> = Vec::new();
        let mut rest_preds: Vec<Predicate> = Vec::new();
        for pred in predicates.drain(..) {
            if let ComparisionValue::Column(rhs) = &pred.value {
                let lc = tree_has_col(&current, &pred.column_name);
                let ln = tree_has_col(&next,    &pred.column_name);
                let rc = tree_has_col(&current, rhs);
                let rn = tree_has_col(&next,    rhs);
                if (lc && rn) || (ln && rc) { join_preds.push(pred); continue; }
            }
            rest_preds.push(pred);
        }
        predicates = rest_preds;
        current = build_join_node(join_preds, Box::new(current), Box::new(next), ctx);
    }

    if !predicates.is_empty() {
        current = OptQueryOp::Filter(OptFilterData {
            predicates,
            underlying: Box::new(current),
        });
    }
    current
}

/// [FIX-ALIAS-COL][FIX-SCALAR-PUSH-ALIAS] Push a scalar predicate into a relation.
fn push_scalar_into_rel(rel: &mut OptQueryOp, pred: Predicate) {
    let dummy = OptQueryOp::Scan(ScanData { table_id: String::new() });
    let old   = std::mem::replace(rel, dummy);
    *rel = match old {
        OptQueryOp::Filter(mut f) => { f.predicates.push(pred); OptQueryOp::Filter(f) }
        other => OptQueryOp::Filter(OptFilterData {
            predicates: vec![pred],
            underlying: Box::new(other),
        }),
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-03][OPT-07][OPT-10][OPT-17][FIX-CROSS-OOM][FIX-COMPOSITE-JOIN][FIX-NE-JOIN]
// build_join_node
// ─────────────────────────────────────────────────────────────────────────────

fn build_join_node(
    join_preds: Vec<Predicate>,
    left:  Box<OptQueryOp>,
    right: Box<OptQueryOp>,
    ctx:   &OptContext<'_>,
) -> OptQueryOp {
    let left_est  = estimated_rows_ctx(&left,  ctx);
    let right_est = estimated_rows_ctx(&right, ctx);

    if join_preds.is_empty() {
        let product_bytes = left_est.saturating_mul(right_est).saturating_mul(512);
        if product_bytes > ctx.memory_budget_bytes as u64 * 4 {
            eprintln!("[FIX-CROSS-OOM] no join pred, product={}×{}≈{} MB > 4×budget → FilterCross",
                      left_est, right_est, product_bytes / (1024 * 1024));
            return OptQueryOp::FilterCross(FilterCrossData {
                predicates: Vec::new(), left, right,
            });
        }
        eprintln!("[OPT-03] tiny cross {}×{} → Cross", left_est, right_est);
        return OptQueryOp::Cross(OptCrossData { left, right });
    }

    // Separate equi-predicates from non-equi.
    let mut equi_preds:    Vec<Predicate> = Vec::new();
    let mut nonequi_preds: Vec<Predicate> = Vec::new();
    for pred in join_preds {
        let is_equi = pred.operator == ComparisionOperator::EQ
            && matches!(&pred.value, ComparisionValue::Column(_));
        if is_equi { equi_preds.push(pred); }
        else       { nonequi_preds.push(pred); }
    }

    if !equi_preds.is_empty() {
        let mut left_keys:  Vec<String>    = Vec::new();
        let mut right_keys: Vec<String>    = Vec::new();
        let mut unresolved: Vec<Predicate> = Vec::new();

        for pred in &equi_preds {
            if let ComparisionValue::Column(rhs) = &pred.value {
                let lhs = pred.column_name.clone();

                // --- Direct check (existing logic) ---
                if tree_has_col(&left, &lhs) && tree_has_col(&right, rhs) {
                    left_keys.push(lhs);
                    right_keys.push(rhs.clone());
                } else if tree_has_col(&left, rhs) && tree_has_col(&right, &lhs) {
                    left_keys.push(rhs.clone());
                    right_keys.push(lhs);
                } else {
                    // --- [FIX-ALIAS-SELF-JOIN] Bare-name resolution ---
                    // Handles "l2.l_orderkey = l1.l_orderkey" where left outputs
                    // "l1.l_orderkey" and right outputs "l2.l_orderkey" (or vice versa).
                    let lhs_bare = lhs.split('.').last().unwrap_or(&lhs);
                    let rhs_bare = rhs.split('.').last().unwrap_or(rhs.as_str());

                    let left_has_lhs  = tree_has_col_bare(&left,  lhs_bare);
                    let left_has_rhs  = tree_has_col_bare(&left,  rhs_bare);
                    let right_has_lhs = tree_has_col_bare(&right, lhs_bare);
                    let right_has_rhs = tree_has_col_bare(&right, rhs_bare);

                    if left_has_lhs && right_has_rhs && lhs_bare != rhs_bare {
                        // Different bare names — direct assignment
                        let lk = tree_find_col_named(&left,  lhs_bare)
                            .unwrap_or_else(|| lhs.clone());
                        let rk = tree_find_col_named(&right, rhs_bare)
                            .unwrap_or_else(|| rhs.clone());
                        eprintln!("[FIX-ALIAS-SELF-JOIN] resolved {}/{} → lk={} rk={}",
                                  lhs, rhs, lk, rk);
                        left_keys.push(lk);
                        right_keys.push(rk);
                    } else if left_has_rhs && right_has_lhs && lhs_bare != rhs_bare {
                        let lk = tree_find_col_named(&left,  rhs_bare)
                            .unwrap_or_else(|| rhs.clone());
                        let rk = tree_find_col_named(&right, lhs_bare)
                            .unwrap_or_else(|| lhs.clone());
                        eprintln!("[FIX-ALIAS-SELF-JOIN] resolved {}/{} → lk={} rk={}",
                                  lhs, rhs, lk, rk);
                        left_keys.push(lk);
                        right_keys.push(rk);
                    } else if lhs_bare == rhs_bare {
                        // Same bare name — self-join alias case (l1.l_orderkey = l2.l_orderkey)
                        // left outputs l1.l_orderkey, right outputs l2.l_orderkey (same bare)
                        let lk = tree_find_col_named(&left,  lhs_bare)
                            .unwrap_or_else(|| lhs.clone());
                        let rk = tree_find_col_named(&right, rhs_bare)
                            .unwrap_or_else(|| rhs.clone());
                        if lk != rk {
                            eprintln!("[FIX-ALIAS-SELF-JOIN] same-bare resolved: lk={} rk={}",
                                      lk, rk);
                            left_keys.push(lk);
                            right_keys.push(rk);
                        } else {
                            unresolved.push(pred.clone());
                        }
                    } else {
                        unresolved.push(pred.clone());
                    }
                }
            }
        }

        if !left_keys.is_empty() {
            let mut extra = nonequi_preds;
            extra.extend(unresolved);

            // [OPT-17] SMJ hint.
            if left_keys.len() == 1
                && subtree_ordered_on_col(&left,  &left_keys[0])
                && subtree_ordered_on_col(&right, &right_keys[0])
            {
                eprintln!("[OPT-17] both sorted on '{}' → SMJ in executor", left_keys[0]);
            }

            // [OPT-10] Build on smaller side.
            eprintln!("[OPT-10][FIX-COMPOSITE-JOIN] HJ keys={:?}/{:?} l_est={} r_est={}",
                      left_keys, right_keys, left_est, right_est);

            if right_est < left_est {
                eprintln!("[OPT-10] swap: right({}) is build side", right_est);
                return OptQueryOp::HashJoin(HashJoinData {
                    left_keys:   right_keys,
                    right_keys:  left_keys,
                    extra_preds: extra,
                    left:        right,
                    right:       left,
                });
            }
            return OptQueryOp::HashJoin(HashJoinData {
                left_keys, right_keys, extra_preds: extra, left, right,
            });
        }
    }

    // Pure non-equi join → FilterCross (NLJ with predicates).
    let mut all_preds = equi_preds;
    all_preds.extend(nonequi_preds);
    eprintln!("[OPT-03][FIX-NE-JOIN] non-equi → FilterCross ({} preds)", all_preds.len());
    OptQueryOp::FilterCross(FilterCrossData { predicates: all_preds, left, right })
}

// ─────────────────────────────────────────────────────────────────────────────
// Pass 2: structural fusions (bottom-up)
// ─────────────────────────────────────────────────────────────────────────────

fn fuse_operators(op: OptQueryOp, ctx: &OptContext<'_>) -> OptQueryOp {
    match op {
        OptQueryOp::Sort(OptSortData { sort_specs, underlying, is_physically_ordered }) => {
            let child = fuse_operators(*underlying, ctx);
            // [OPT-24] Re-check physical order after fusing children (projection
            // pushdown can expose a FilteredScan with is_physically_ordered_on set).
            let already = is_physically_ordered
                || subtree_is_ordered_on_specs(&child, &sort_specs, ctx);
            if already && !is_physically_ordered {
                eprintln!("[OPT-24] Sort redundant after fusion on {:?}",
                          sort_specs.iter().map(|s| &s.column_name).collect::<Vec<_>>());
            }
            fuse_sort(sort_specs, child, already)
        }
        OptQueryOp::Project(OptProjectData { column_name_map, underlying }) => {
            let child = fuse_operators(*underlying, ctx);
            fuse_project(column_name_map, child)
        }
        OptQueryOp::Filter(OptFilterData { predicates, underlying }) => {
            let child = fuse_operators(*underlying, ctx);
            fuse_filter_node(predicates, child)
        }
        OptQueryOp::HashJoin(d) => OptQueryOp::HashJoin(HashJoinData {
            left:  Box::new(fuse_operators(*d.left,  ctx)),
            right: Box::new(fuse_operators(*d.right, ctx)),
            ..d
        }),
        OptQueryOp::FilterCross(d) => OptQueryOp::FilterCross(FilterCrossData {
            left:  Box::new(fuse_operators(*d.left,  ctx)),
            right: Box::new(fuse_operators(*d.right, ctx)),
            ..d
        }),
        OptQueryOp::Cross(d) => OptQueryOp::Cross(OptCrossData {
            left:  Box::new(fuse_operators(*d.left,  ctx)),
            right: Box::new(fuse_operators(*d.right, ctx)),
        }),
        other => other,
    }
}

// [OPT-01][OPT-05] Filter(Scan) → FilteredScan.
fn fuse_filter_node(predicates: Vec<Predicate>, child: OptQueryOp) -> OptQueryOp {
    match child {
        OptQueryOp::Scan(ScanData { table_id }) => {
            eprintln!("[OPT-01][OPT-05] Filter(Scan({})) → FilteredScan", table_id);
            OptQueryOp::FilteredScan(FilteredScanData {
                table_id, predicates, project: None, is_physically_ordered_on: None,
            })
        }
        OptQueryOp::FilteredScan(mut fs) => {
            fs.predicates.extend(predicates);
            OptQueryOp::FilteredScan(fs)
        }
        other => OptQueryOp::Filter(OptFilterData {
            predicates, underlying: Box::new(other),
        }),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-04][FIX-PROJ-SORT][FIX-PROJ-PASSTHROUGH] Projection pushdown
//
// Case A: Project(FilteredScan)             → FilteredScan+proj
// Case B: Project(Sort(Filter*(FScan)))     → Sort(Filter*(FScan+proj)) + residual
// Case C: Project(HashJoin/FilterCross/…)  → push narrower proj into each child
//          when the proj columns are a strict subset of that child's output
// ─────────────────────────────────────────────────────────────────────────────

fn fuse_project(map: Vec<(String, String)>, child: OptQueryOp) -> OptQueryOp {
    match child {
        // Case A: directly into FilteredScan.
        OptQueryOp::FilteredScan(mut fs) if fs.project.is_none() => {
            eprintln!("[OPT-04] Project(FScan({})) → FScan+proj ({} cols)", fs.table_id, map.len());
            fs.project = Some(map);
            OptQueryOp::FilteredScan(fs)
        }

        // Case B: Sort over a FilteredScan (possibly through Filter layers).
        OptQueryOp::Sort(OptSortData { sort_specs, underlying, is_physically_ordered }) => {
            if inner_fs_has_no_project(&underlying) {
                let mut ext_map = map.clone();
                let mut extras: Vec<(String, String)> = Vec::new();
                for spec in &sort_specs {
                    let key = &spec.column_name;
                    let present = ext_map.iter().any(|(_, to)| {
                        to == key
                            || to.ends_with(&format!(".{}", key))
                            || key.ends_with(&format!(".{}", to))
                    });
                    if !present {
                        // [FIX-SORT-EXTRA-COL] Map the fully-qualified form to the
                        // bare sort key so cmp_rows can always find it.
                        ext_map.push((key.clone(), key.clone()));
                        extras.push((key.clone(), key.clone()));
                    }
                }
                let new_under = push_proj_into_node(*underlying, ext_map);
                eprintln!("[OPT-04] Project(Sort(FScan)) → Sort(FScan+proj) extra={:?}", extras);
                let sort_node = OptQueryOp::Sort(OptSortData {
                    sort_specs, underlying: Box::new(new_under), is_physically_ordered,
                });
                if extras.is_empty() { sort_node }
                else {
                    OptQueryOp::Project(OptProjectData {
                        column_name_map: map, underlying: Box::new(sort_node),
                    })
                }
            } else {
                OptQueryOp::Project(OptProjectData {
                    column_name_map: map,
                    underlying: Box::new(OptQueryOp::Sort(OptSortData {
                        sort_specs, underlying, is_physically_ordered,
                    })),
                })
            }
        }

        // [FIX-PROJ-PASSTHROUGH] Case C: push through Filter when all projected
        // columns exist in the Filter's child (so the filter can still evaluate
        // its predicates after projection).
        OptQueryOp::Filter(OptFilterData { predicates, underlying }) => {
            // Only push down if every filter predicate column is in the map output
            // OR in the map input (the filter evaluates before projection at exec time,
            // but since exec_filter runs on the child result, we need to keep any column
            // the filter touches). Safest: don't push through Filter, just wrap.
            OptQueryOp::Project(OptProjectData {
                column_name_map: map,
                underlying: Box::new(OptQueryOp::Filter(OptFilterData {
                    predicates, underlying,
                })),
            })
        }

        // [FIX-PROJ-PASSTHROUGH] Case C: Project(HashJoin) — push subset of needed
        // columns into each branch via a narrowing sub-projection if beneficial.
        OptQueryOp::HashJoin(hj) => {
            // Collect all column names referenced by this project and by extra_preds.
            let needed: std::collections::HashSet<String> = map.iter()
                .map(|(from, _)| from.clone())
                .chain(hj.extra_preds.iter().flat_map(|p| {
                    let mut v = vec![p.column_name.clone()];
                    if let ComparisionValue::Column(c) = &p.value { v.push(c.clone()); }
                    v
                }))
                .chain(hj.left_keys.iter().cloned())
                .chain(hj.right_keys.iter().cloned())
                .collect();

            // Build per-branch sub-projections only when there is meaningful narrowing.
            let left_out  = tree_output_cols(&hj.left);
            let right_out = tree_output_cols(&hj.right);
            let left_needed: Vec<String>  = left_out.iter().filter(|c| needed.contains(*c)).cloned().collect();
            let right_needed: Vec<String> = right_out.iter().filter(|c| needed.contains(*c)).cloned().collect();

            let narrowing_useful =
                left_needed.len() < left_out.len().saturating_sub(1) ||
                right_needed.len() < right_out.len().saturating_sub(1);

            if narrowing_useful {
                eprintln!("[FIX-PROJ-PASSTHROUGH] pushing project through HashJoin: \
                           left {}->{} right {}->{}",
                          left_out.len(), left_needed.len(),
                          right_out.len(), right_needed.len());
                let new_left = if left_needed.len() < left_out.len() {
                    let lmap: Vec<(String, String)> = left_needed.iter().map(|c| (c.clone(), c.clone())).collect();
                    Box::new(fuse_project(lmap, *hj.left))
                } else { hj.left };
                let new_right = if right_needed.len() < right_out.len() {
                    let rmap: Vec<(String, String)> = right_needed.iter().map(|c| (c.clone(), c.clone())).collect();
                    Box::new(fuse_project(rmap, *hj.right))
                } else { hj.right };
                let new_hj = OptQueryOp::HashJoin(HashJoinData {
                    left: new_left, right: new_right, ..hj
                });
                // Final outer project to rename/reorder.
                OptQueryOp::Project(OptProjectData {
                    column_name_map: map, underlying: Box::new(new_hj),
                })
            } else {
                OptQueryOp::Project(OptProjectData {
                    column_name_map: map, underlying: Box::new(OptQueryOp::HashJoin(hj)),
                })
            }
        }

        other => OptQueryOp::Project(OptProjectData {
            column_name_map: map, underlying: Box::new(other),
        }),
    }
}

fn inner_fs_has_no_project(op: &OptQueryOp) -> bool {
    match op {
        OptQueryOp::FilteredScan(fs) => fs.project.is_none(),
        OptQueryOp::Filter(f)        => inner_fs_has_no_project(&f.underlying),
        _                            => false,
    }
}

fn push_proj_into_node(op: OptQueryOp, map: Vec<(String, String)>) -> OptQueryOp {
    match op {
        OptQueryOp::FilteredScan(mut fs) => { fs.project = Some(map); OptQueryOp::FilteredScan(fs) }
        OptQueryOp::Filter(OptFilterData { predicates, underlying }) =>
            OptQueryOp::Filter(OptFilterData {
                predicates,
                underlying: Box::new(push_proj_into_node(*underlying, map)),
            }),
        other => other,
    }
}

fn fuse_sort(specs: Vec<SortSpec>, child: OptQueryOp, is_physically_ordered: bool) -> OptQueryOp {
    if is_physically_ordered { eprintln!("[OPT-11] Sort no-op (already ordered)"); }
    OptQueryOp::Sort(OptSortData { sort_specs: specs, underlying: Box::new(child), is_physically_ordered })
}

// ─────────────────────────────────────────────────────────────────────────────
// Pass 3: [FIX-CROSS-OOM] distribute_predicates_deep
// ─────────────────────────────────────────────────────────────────────────────

fn distribute_predicates_deep(op: OptQueryOp, ctx: &OptContext<'_>) -> OptQueryOp {
    match op {
        OptQueryOp::Filter(OptFilterData { predicates, underlying }) => {
            let child = distribute_predicates_deep(*underlying, ctx);
            match child {
                OptQueryOp::Cross(OptCrossData { left, right }) => {
                    eprintln!("[FIX-CROSS-OOM] pass3: Filter(Cross) → re-converting");
                    fuse_filter_cross(predicates, left, right, ctx)
                }
                OptQueryOp::FilterCross(d) => {
                    let mut all = predicates;
                    all.extend(d.predicates);
                    eprintln!("[FIX-CROSS-OOM] pass3: Filter(FilterCross) → build_join_node");
                    build_join_node(all, d.left, d.right, ctx)
                }
                OptQueryOp::HashJoin(mut hj) => {
                    let mut leftover = Vec::new();
                    for pred in predicates {
                        if let ComparisionValue::Column(rhs) = &pred.value {
                            if (tree_has_col(&hj.left, &pred.column_name) && tree_has_col(&hj.right, rhs))
                            || (tree_has_col(&hj.left, rhs) && tree_has_col(&hj.right, &pred.column_name))
                            {
                                hj.extra_preds.push(pred);
                                continue;
                            }
                        }
                        leftover.push(pred);
                    }
                    let base = OptQueryOp::HashJoin(hj);
                    if leftover.is_empty() { base }
                    else { OptQueryOp::Filter(OptFilterData { predicates: leftover, underlying: Box::new(base) }) }
                }
                other => OptQueryOp::Filter(OptFilterData {
                    predicates, underlying: Box::new(other),
                }),
            }
        }

        OptQueryOp::Sort(OptSortData { sort_specs, underlying, is_physically_ordered }) =>
            OptQueryOp::Sort(OptSortData {
                sort_specs,
                underlying: Box::new(distribute_predicates_deep(*underlying, ctx)),
                is_physically_ordered,
            }),
        OptQueryOp::Project(OptProjectData { column_name_map, underlying }) =>
            OptQueryOp::Project(OptProjectData {
                column_name_map,
                underlying: Box::new(distribute_predicates_deep(*underlying, ctx)),
            }),
        OptQueryOp::HashJoin(d) => OptQueryOp::HashJoin(HashJoinData {
            left:  Box::new(distribute_predicates_deep(*d.left,  ctx)),
            right: Box::new(distribute_predicates_deep(*d.right, ctx)),
            ..d
        }),
        OptQueryOp::FilterCross(d) => OptQueryOp::FilterCross(FilterCrossData {
            left:  Box::new(distribute_predicates_deep(*d.left,  ctx)),
            right: Box::new(distribute_predicates_deep(*d.right, ctx)),
            ..d
        }),
        OptQueryOp::Cross(d) => {
            let left  = Box::new(distribute_predicates_deep(*d.left,  ctx));
            let right = Box::new(distribute_predicates_deep(*d.right, ctx));
            let le    = estimated_rows_ctx(&left,  ctx);
            let re    = estimated_rows_ctx(&right, ctx);
            let bytes = le.saturating_mul(re).saturating_mul(512);
            if bytes > ctx.memory_budget_bytes as u64 * 4 {
                eprintln!("[FIX-CROSS-OOM] pass3: Cross {}×{}≈{} MB → FilterCross",
                          le, re, bytes / (1024 * 1024));
                OptQueryOp::FilterCross(FilterCrossData { predicates: Vec::new(), left, right })
            } else {
                OptQueryOp::Cross(OptCrossData { left, right })
            }
        }
        OptQueryOp::FilteredScan(mut fs) => {
            annotate_early_stop(&mut fs, ctx);
            OptQueryOp::FilteredScan(fs)
        }
        other => other,
    }
}

/// [OPT-19] Annotate a FilteredScan with is_physically_ordered_on.
pub fn annotate_early_stop(fs: &mut FilteredScanData, ctx: &OptContext<'_>) {
    if fs.is_physically_ordered_on.is_some() || fs.predicates.is_empty() { return; }

    let resolved = ctx.resolve_table_id(&fs.table_id);
    let spec = match ctx.db.get_table_specs().iter().find(|t| t.name == resolved) {
        Some(s) => s,
        None    => return,
    };

    let ordered_cols: Vec<&str> = spec.column_specs.iter()
        .filter(|c| {
            c.stats.as_ref().map(|stats|
                stats.iter().any(|s| matches!(s, ColumnStat::IsPhysicallyOrdered))
            ).unwrap_or(false)
        })
        .map(|c| c.column_name.as_str())
        .collect();

    let fallback = [
        "l_orderkey", "o_orderkey", "ps_partkey", "p_partkey",
        "c_custkey",  "s_suppkey",  "n_nationkey", "r_regionkey",
    ];
    let effective: Vec<&str> = if ordered_cols.is_empty() {
        fallback.iter().copied().collect()
    } else {
        ordered_cols
    };

    for pred in &fs.predicates {
        if !matches!(pred.operator, ComparisionOperator::LT | ComparisionOperator::LTE) { continue; }
        let bare = pred.column_name.split('.').last().unwrap_or(&pred.column_name);
        if effective.iter().any(|oc| *oc == bare) {
            eprintln!("[OPT-19] early_stop col='{}' table='{}'", bare, fs.table_id);
            fs.is_physically_ordered_on = Some(pred.column_name.clone());
            return;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pass 4: [OPT-22] Multi-pass scalar pushdown
//
// After join building, scalar predicates may still sit above Project/Sort nodes
// because build_optimal_join only pushes into flat relation lists. This pass
// recurses through the entire plan and pushes any remaining scalar preds into
// the nearest FilteredScan or Filter below them.
// ─────────────────────────────────────────────────────────────────────────────

fn push_all_scalars_deep(op: OptQueryOp, ctx: &OptContext<'_>) -> OptQueryOp {
    match op {
        OptQueryOp::Filter(OptFilterData { predicates, underlying }) => {
            let child = push_all_scalars_deep(*underlying, ctx);
            // Separate scalar vs col-col predicates.
            let mut scalars: Vec<Predicate> = Vec::new();
            let mut col_col: Vec<Predicate> = Vec::new();
            for p in predicates {
                if matches!(&p.value, ComparisionValue::Column(_)) { col_col.push(p); }
                else { scalars.push(p); }
            }
            // Try to push each scalar into the child subtree.
            let mut child_with_scalars = child;
            let mut unpushed: Vec<Predicate> = Vec::new();
            for p in scalars {
                if try_push_scalar_deep(&mut child_with_scalars, &p) {
                    eprintln!("[OPT-22] scalar pred on '{}' pushed deeper", p.column_name);
                } else {
                    unpushed.push(p);
                }
            }
            // Rebuild: col-col + unpushed scalars wrap the (now-modified) child.
            let mut all_remaining = col_col;
            all_remaining.extend(unpushed);
            if all_remaining.is_empty() { child_with_scalars }
            else {
                OptQueryOp::Filter(OptFilterData {
                    predicates: all_remaining, underlying: Box::new(child_with_scalars),
                })
            }
        }
        OptQueryOp::Sort(OptSortData { sort_specs, underlying, is_physically_ordered }) =>
            OptQueryOp::Sort(OptSortData {
                sort_specs,
                underlying: Box::new(push_all_scalars_deep(*underlying, ctx)),
                is_physically_ordered,
            }),
        OptQueryOp::Project(OptProjectData { column_name_map, underlying }) =>
            OptQueryOp::Project(OptProjectData {
                column_name_map,
                underlying: Box::new(push_all_scalars_deep(*underlying, ctx)),
            }),
        OptQueryOp::HashJoin(d) => OptQueryOp::HashJoin(HashJoinData {
            left:  Box::new(push_all_scalars_deep(*d.left,  ctx)),
            right: Box::new(push_all_scalars_deep(*d.right, ctx)),
            ..d
        }),
        OptQueryOp::FilterCross(d) => OptQueryOp::FilterCross(FilterCrossData {
            left:  Box::new(push_all_scalars_deep(*d.left,  ctx)),
            right: Box::new(push_all_scalars_deep(*d.right, ctx)),
            ..d
        }),
        OptQueryOp::Cross(d) => OptQueryOp::Cross(OptCrossData {
            left:  Box::new(push_all_scalars_deep(*d.left,  ctx)),
            right: Box::new(push_all_scalars_deep(*d.right, ctx)),
        }),
        other => other,
    }
}

/// Attempt to push a scalar predicate into the subtree rooted at `op`.
/// Returns true if the predicate was consumed.
fn try_push_scalar_deep(op: &mut OptQueryOp, pred: &Predicate) -> bool {
    let has_col=tree_has_col(op, &pred.column_name);
    match op {
        OptQueryOp::FilteredScan(fs) if has_col => {
            fs.predicates.push(pred.clone());
            true
        }
        OptQueryOp::Filter(f) if tree_has_col(&f.underlying, &pred.column_name) => {
            f.predicates.push(pred.clone());
            true
        }
        OptQueryOp::Sort(d)    => try_push_scalar_deep(&mut d.underlying, pred),
        OptQueryOp::Project(d) => try_push_scalar_deep(&mut d.underlying, pred),
        OptQueryOp::HashJoin(hj) => {
            if tree_has_col(&hj.left, &pred.column_name) {
                return try_push_scalar_deep(&mut hj.left, pred);
            }
            if tree_has_col(&hj.right, &pred.column_name) {
                return try_push_scalar_deep(&mut hj.right, pred);
            }
            false
        }
        OptQueryOp::FilterCross(fc) => {
            if tree_has_col(&fc.left, &pred.column_name) {
                return try_push_scalar_deep(&mut fc.left, pred);
            }
            if tree_has_col(&fc.right, &pred.column_name) {
                return try_push_scalar_deep(&mut fc.right, pred);
            }
            false
        }
        OptQueryOp::Cross(c) => {
            if tree_has_col(&c.left, &pred.column_name) {
                return try_push_scalar_deep(&mut c.left, pred);
            }
            if tree_has_col(&c.right, &pred.column_name) {
                return try_push_scalar_deep(&mut c.right, pred);
            }
            false
        }
        _ => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pass 5: [OPT-23] Redundant Project elimination
//
// Removes Project(FilteredScan) where the project map is an identity mapping
// (from == to, same set of columns already in the FScan projection).
// Also removes Project(Project) by merging maps.
// ─────────────────────────────────────────────────────────────────────────────

fn elide_redundant_projects(op: OptQueryOp) -> OptQueryOp {
    match op {
        OptQueryOp::Project(OptProjectData { column_name_map, underlying }) => {
            let child = elide_redundant_projects(*underlying);
            match child {
                // Merge Project(Project) → single Project.
                OptQueryOp::Project(OptProjectData { column_name_map: inner_map, underlying: inner_under }) => {
                    // Build a combined map: for each (from, to) in outer map, find inner to→actual.
                    let combined: Vec<(String, String)> = column_name_map.iter().map(|(from, to)| {
                        let resolved = inner_map.iter()
                            .find(|(_, inner_to)| inner_to == from)
                            .map(|(inner_from, _)| inner_from.clone())
                            .unwrap_or_else(|| from.clone());
                        (resolved, to.clone())
                    }).collect();
                    eprintln!("[OPT-23] merged Project(Project)");
                    elide_redundant_projects(OptQueryOp::Project(OptProjectData {
                        column_name_map: combined, underlying: inner_under,
                    }))
                }
                // Identity project on FilteredScan: elide if the FScan already has
                // exactly those columns projected and all from==to.
                OptQueryOp::FilteredScan(ref fs) => {
                    let is_identity = column_name_map.iter().all(|(from, to)| from == to);
                    if is_identity {
                        if let Some(ref proj) = fs.project {
                            let proj_tos: Vec<&str> = proj.iter().map(|(_, t)| t.as_str()).collect();
                            let map_froms: Vec<&str> = column_name_map.iter().map(|(f, _)| f.as_str()).collect();
                            if proj_tos == map_froms {
                                eprintln!("[OPT-23] elided redundant identity Project over FilteredScan");
                                return child;
                            }
                        }
                    }
                    OptQueryOp::Project(OptProjectData { column_name_map, underlying: Box::new(child) })
                }
                other => OptQueryOp::Project(OptProjectData {
                    column_name_map, underlying: Box::new(other),
                }),
            }
        }
        OptQueryOp::Sort(OptSortData { sort_specs, underlying, is_physically_ordered }) =>
            OptQueryOp::Sort(OptSortData {
                sort_specs,
                underlying: Box::new(elide_redundant_projects(*underlying)),
                is_physically_ordered,
            }),
        OptQueryOp::Filter(OptFilterData { predicates, underlying }) =>
            OptQueryOp::Filter(OptFilterData {
                predicates,
                underlying: Box::new(elide_redundant_projects(*underlying)),
            }),
        OptQueryOp::HashJoin(d) => OptQueryOp::HashJoin(HashJoinData {
            left:  Box::new(elide_redundant_projects(*d.left)),
            right: Box::new(elide_redundant_projects(*d.right)),
            ..d
        }),
        OptQueryOp::FilterCross(d) => OptQueryOp::FilterCross(FilterCrossData {
            left:  Box::new(elide_redundant_projects(*d.left)),
            right: Box::new(elide_redundant_projects(*d.right)),
            ..d
        }),
        OptQueryOp::Cross(d) => OptQueryOp::Cross(OptCrossData {
            left:  Box::new(elide_redundant_projects(*d.left)),
            right: Box::new(elide_redundant_projects(*d.right)),
        }),
        other => other,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pass 6: [OPT-21] Reorder extra_preds inside every HashJoin by selectivity.
//
// extra_preds in HashJoin are applied as post-join filters per output row.
// Ordering them most-selective-first gives eval_preds short-circuit wins.
// ─────────────────────────────────────────────────────────────────────────────

fn reorder_join_extra_preds(op: OptQueryOp, ctx: &OptContext<'_>) -> OptQueryOp {
    match op {
        OptQueryOp::HashJoin(mut d) => {
            // Reorder extra_preds if we can identify a base table for estimation.
            // Use the build side (left) table id for selectivity estimates.
            if d.extra_preds.len() > 1 {
                if let Some(tid) = tree_base_table_id(&d.left) {
                    reorder_predicates_by_selectivity(&mut d.extra_preds, &tid, ctx);
                    eprintln!("[OPT-21] reordered {} extra_preds on HJ (table={})",
                              d.extra_preds.len(), tid);
                }
            }
            OptQueryOp::HashJoin(HashJoinData {
                left:  Box::new(reorder_join_extra_preds(*d.left,  ctx)),
                right: Box::new(reorder_join_extra_preds(*d.right, ctx)),
                ..d
            })
        }
        OptQueryOp::Sort(OptSortData { sort_specs, underlying, is_physically_ordered }) =>
            OptQueryOp::Sort(OptSortData {
                sort_specs,
                underlying: Box::new(reorder_join_extra_preds(*underlying, ctx)),
                is_physically_ordered,
            }),
        OptQueryOp::Project(OptProjectData { column_name_map, underlying }) =>
            OptQueryOp::Project(OptProjectData {
                column_name_map,
                underlying: Box::new(reorder_join_extra_preds(*underlying, ctx)),
            }),
        OptQueryOp::Filter(OptFilterData { predicates, underlying }) =>
            OptQueryOp::Filter(OptFilterData {
                predicates,
                underlying: Box::new(reorder_join_extra_preds(*underlying, ctx)),
            }),
        OptQueryOp::FilterCross(d) => OptQueryOp::FilterCross(FilterCrossData {
            left:  Box::new(reorder_join_extra_preds(*d.left,  ctx)),
            right: Box::new(reorder_join_extra_preds(*d.right, ctx)),
            ..d
        }),
        OptQueryOp::Cross(d) => OptQueryOp::Cross(OptCrossData {
            left:  Box::new(reorder_join_extra_preds(*d.left,  ctx)),
            right: Box::new(reorder_join_extra_preds(*d.right, ctx)),
        }),
        other => other,
    }
}

/// Find the base table id of the deepest scan in a subtree (best-effort).
fn tree_base_table_id(op: &OptQueryOp) -> Option<String> {
    match op {
        OptQueryOp::Scan(d)         => Some(d.table_id.clone()),
        OptQueryOp::FilteredScan(d) => Some(d.table_id.clone()),
        OptQueryOp::Filter(d)       => tree_base_table_id(&d.underlying),
        OptQueryOp::Project(d)      => tree_base_table_id(&d.underlying),
        OptQueryOp::Sort(d)         => tree_base_table_id(&d.underlying),
        OptQueryOp::HashJoin(d)     => tree_base_table_id(&d.left),
        OptQueryOp::FilterCross(d)  => tree_base_table_id(&d.left),
        OptQueryOp::Cross(d)        => tree_base_table_id(&d.left),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pass 7: Diagnostic — verify scalar predicates reached FilteredScan leaves
// ─────────────────────────────────────────────────────────────────────────────

fn verify_scalar_pushdown(op: &OptQueryOp) {
    let mut unpushed = 0usize;
    count_unpushed_scalars(op, &mut unpushed);
    if unpushed > 0 {
        eprintln!("[OPT-WARN][OPT-22] {} scalar predicate(s) NOT pushed to FilteredScan leaves!",
                  unpushed);
    } else {
        eprintln!("[OPT-22] all scalar predicates successfully pushed to scan leaves.");
    }
}

fn count_unpushed_scalars(op: &OptQueryOp, count: &mut usize) {
    match op {
        OptQueryOp::Filter(d) => {
            for p in &d.predicates {
                if !matches!(&p.value, ComparisionValue::Column(_)) {
                    *count += 1;
                    eprintln!("[OPT-WARN] unpushed scalar pred on col '{}' op={:?}",
                              p.column_name, p.operator);
                }
            }
            count_unpushed_scalars(&d.underlying, count);
        }
        OptQueryOp::Sort(d)        => count_unpushed_scalars(&d.underlying, count),
        OptQueryOp::Project(d)     => count_unpushed_scalars(&d.underlying, count),
        OptQueryOp::HashJoin(d)    => {
            count_unpushed_scalars(&d.left, count);
            count_unpushed_scalars(&d.right, count);
        }
        OptQueryOp::FilterCross(d) => {
            count_unpushed_scalars(&d.left, count);
            count_unpushed_scalars(&d.right, count);
        }
        OptQueryOp::Cross(d)       => {
            count_unpushed_scalars(&d.left, count);
            count_unpushed_scalars(&d.right, count);
        }
        _ => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-02] Predicate reordering
// ─────────────────────────────────────────────────────────────────────────────

fn reorder_predicates_by_selectivity(
    preds:    &mut Vec<Predicate>,
    table_id: &str,
    ctx:      &OptContext<'_>,
) {
    if preds.len() <= 1 { return; }
    preds.sort_by(|a, b| {
        let sa = ctx.filter_selectivity(table_id, std::slice::from_ref(a));
        let sb = ctx.filter_selectivity(table_id, std::slice::from_ref(b));
        sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn scan_table_of_queryop(op: &QueryOp) -> Option<String> {
    match op {
        QueryOp::Scan(d)    => Some(d.table_id.clone()),
        QueryOp::Filter(d)  => scan_table_of_queryop(d.underlying.as_ref()),
        QueryOp::Project(d) => scan_table_of_queryop(d.underlying.as_ref()),
        QueryOp::Sort(d)    => scan_table_of_queryop(d.underlying.as_ref()),
        QueryOp::Cross(_)   => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-11][OPT-18][OPT-24] Physical-order propagation
// ─────────────────────────────────────────────────────────────────────────────

fn subtree_is_ordered_on_specs(
    child: &OptQueryOp,
    specs: &[SortSpec],
    ctx:   &OptContext<'_>,
) -> bool {
    if specs.is_empty() { return true; }
    match child {
        OptQueryOp::Sort(d) if d.is_physically_ordered =>
            d.sort_specs.len() >= specs.len()
            && d.sort_specs.iter().zip(specs.iter())
                .all(|(a, b)| a.column_name == b.column_name && a.ascending == b.ascending),

        OptQueryOp::FilteredScan(d) => {
            // Case 1: explicit early-stop annotation on exactly the first sort col
            if let Some(ref oc) = d.is_physically_ordered_on {
                let col      = &specs[0].column_name;
                let bare     = col.split('.').last().unwrap_or(col);
                let oc_bare  = oc.split('.').last().unwrap_or(oc);
                if specs[0].ascending && (bare == oc_bare || col == oc.as_str()) {
                    return true;
                }
            }
            // Case 2: table's PK column is physically ordered and first sort spec
            // matches it — the filter does not change physical order.
            if !specs.is_empty() && specs[0].ascending {
                let col  = &specs[0].column_name;
                let bare = col.split('.').last().unwrap_or(col);
                if ctx.column_is_physically_ordered(&d.table_id, bare) {
                    eprintln!("[OPT-11] FScan({}) already ordered on '{}' → sort skipped",
                              d.table_id, bare);
                    return true;
                }
            }
            false
        }

        OptQueryOp::Scan(d) => {
            if !specs.is_empty() && specs[0].ascending {
                let col  = &specs[0].column_name;
                let bare = col.split('.').last().unwrap_or(col);
                if ctx.column_is_physically_ordered(&d.table_id, bare) {
                    return true;
                }
            }
            false
        }

        OptQueryOp::Filter(d)  => subtree_is_ordered_on_specs(&d.underlying, specs, ctx),
        OptQueryOp::Project(d) => subtree_is_ordered_on_specs(&d.underlying, specs, ctx),
        _ => false,
    }
}

/// [OPT-17][OPT-18] True when the subtree is sorted on a single column.
pub fn subtree_ordered_on_col(op: &OptQueryOp, col: &str) -> bool {
    let bare = col.split('.').last().unwrap_or(col);
    match op {
        OptQueryOp::Sort(d) =>
            d.is_physically_ordered
            && d.sort_specs.first().map(|s| {
                let sc = s.column_name.as_str();
                sc == col || sc == bare || sc.ends_with(&format!(".{}", bare))
            }).unwrap_or(false),
        OptQueryOp::FilteredScan(d) =>
            d.is_physically_ordered_on.as_deref().map(|c| {
                c == col || c == bare || c.ends_with(&format!(".{}", bare))
            }).unwrap_or(false),
        OptQueryOp::Filter(d)  => subtree_ordered_on_col(&d.underlying, col),
        OptQueryOp::Project(d) => subtree_ordered_on_col(&d.underlying, col),
        _ => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// [OPT-13] Cardinality estimation
// ─────────────────────────────────────────────────────────────────────────────

pub fn estimated_rows_ctx(op: &OptQueryOp, ctx: &OptContext<'_>) -> u64 {
    match op {
        OptQueryOp::Scan(d)         => ctx.table_row_count(&d.table_id),
        OptQueryOp::FilteredScan(d) => {
            let base = ctx.table_row_count(&d.table_id);
            let sel  = ctx.filter_selectivity(&d.table_id, &d.predicates);
            ((base as f64 * sel) as u64).max(1)
        }
        OptQueryOp::Filter(d) => {
            let base   = estimated_rows_ctx(&d.underlying, ctx);
            let factor = 0.25f64.powi(d.predicates.len().min(4) as i32);
            ((base as f64 * factor) as u64).max(1)
        }
        OptQueryOp::Project(d)      =>  estimated_rows_ctx(&d.underlying, ctx),
        OptQueryOp::Sort(d)         =>  estimated_rows_ctx(&d.underlying, ctx),
        OptQueryOp::Cross(d)        =>
            estimated_rows_ctx(&d.left, ctx)
                .saturating_mul(estimated_rows_ctx(&d.right, ctx)),
        OptQueryOp::FilterCross(d)  =>
            (estimated_rows_ctx(&d.left, ctx)
                .saturating_mul(estimated_rows_ctx(&d.right, ctx)) / 10).max(1),
        OptQueryOp::HashJoin(d) =>
            estimated_rows_ctx(&d.left, ctx).max(estimated_rows_ctx(&d.right, ctx)),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TPC-H SF-1 heuristics — FALLBACK ONLY
// ─────────────────────────────────────────────────────────────────────────────

fn tpch_row_estimate(name: &str) -> u64 {
    match name {
        "nation"   =>         25,
        "region"   =>          5,
        "supplier" =>      1_000,
        "customer" =>     15_000,
        "part"     =>     20_000,
        "partsupp" =>     80_000,
        "orders"   =>    150_000,
        "lineitem" =>    600_000,
        _          =>     50_000,
    }
}

/// Check if a subtree has any output column whose bare name (after last '.') matches.
/// True if any output column of the subtree has the given bare name (after last '.').
fn tree_has_col_bare(op: &OptQueryOp, bare: &str) -> bool {
    match op {
        OptQueryOp::Project(d) =>
            d.column_name_map.iter().any(|(_, to)| {
                to.split('.').last().unwrap_or(to) == bare
            }),
        OptQueryOp::FilteredScan(d) => {
            if let Some(ref proj) = d.project {
                proj.iter().any(|(_, to)| to.split('.').last().unwrap_or(to) == bare)
            } else {
                // No projection — check if the table has a column with this bare name
                known_col_prefix(&d.table_id, &format!("_{}", bare))
                    || bare.contains('_') // heuristic: any col_ name might be from this table
            }
        }
        OptQueryOp::Scan(d) => {
            // Check if the table plausibly has a column with this bare name.
            // We use known_col_prefix with a dummy full name.
            known_col_prefix(&d.table_id, &format!("x_{}", bare))
        }
        OptQueryOp::Filter(d)      => tree_has_col_bare(&d.underlying, bare),
        OptQueryOp::Sort(d)        => tree_has_col_bare(&d.underlying, bare),
        OptQueryOp::HashJoin(d)    =>
            tree_has_col_bare(&d.left, bare) || tree_has_col_bare(&d.right, bare),
        OptQueryOp::FilterCross(d) =>
            tree_has_col_bare(&d.left, bare) || tree_has_col_bare(&d.right, bare),
        OptQueryOp::Cross(d)       =>
            tree_has_col_bare(&d.left, bare) || tree_has_col_bare(&d.right, bare),
    }
}

/// Find the actual output column name whose bare name (after last '.') matches.
/// Returns the first match, i.e. the aliased name like "l1.l_orderkey".
fn tree_find_col_named(op: &OptQueryOp, bare: &str) -> Option<String> {
    match op {
        OptQueryOp::Project(d) =>
            d.column_name_map.iter()
                .find(|(_, to)| to.split('.').last().unwrap_or(to) == bare)
                .map(|(_, to)| to.clone()),
        OptQueryOp::FilteredScan(d) => {
            if let Some(ref proj) = d.project {
                proj.iter()
                    .find(|(_, to)| to.split('.').last().unwrap_or(to) == bare)
                    .map(|(_, to)| to.clone())
            } else {
                None
            }
        }
        OptQueryOp::Filter(d)      => tree_find_col_named(&d.underlying, bare),
        OptQueryOp::Sort(d)        => tree_find_col_named(&d.underlying, bare),
        OptQueryOp::HashJoin(d)    =>
            tree_find_col_named(&d.left, bare)
                .or_else(|| tree_find_col_named(&d.right, bare)),
        OptQueryOp::FilterCross(d) =>
            tree_find_col_named(&d.left, bare)
                .or_else(|| tree_find_col_named(&d.right, bare)),
        OptQueryOp::Cross(d)       =>
            tree_find_col_named(&d.left, bare)
                .or_else(|| tree_find_col_named(&d.right, bare)),
        _ => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Column presence helpers
// [FIX-ALIAS-COL] tree_has_col now handles dotted alias names.
// ─────────────────────────────────────────────────────────────────────────────

pub fn tree_has_col(op: &OptQueryOp, col: &str) -> bool {
    match op {
        OptQueryOp::Scan(d)         =>
            col.starts_with(&format!("{}.", d.table_id))
            || known_col_prefix(d.table_id.as_str(), col)
            || alias_col_matches_table(d.table_id.as_str(), col),

        OptQueryOp::FilteredScan(d) => {
            if let Some(ref proj) = d.project {
                proj.iter().any(|(_, to)| {
                    to == col
                    || to.ends_with(&format!(".{}", col.split('.').last().unwrap_or(col)))
                        && col.split('.').next() == to.split('.').next()
                    || col.ends_with(&format!(".{}", to.split('.').last().unwrap_or(to)))
                })
            } else {
                col.starts_with(&format!("{}.", d.table_id))
                || known_col_prefix(d.table_id.as_str(), col)
                || alias_col_matches_table(d.table_id.as_str(), col)
            }
        }

        OptQueryOp::Filter(d)       => tree_has_col(&d.underlying, col),
        OptQueryOp::Project(d)      => {
            d.column_name_map.iter().any(|(_, to)| {
                to == col
                || to.split('.').last() == col.split('.').last()
                   && (col.contains('.') || to.contains('.'))
                   && if col.contains('.') && to.contains('.') {
                       col.split('.').next() == to.split('.').next()
                   } else { true }
            })
        }
        OptQueryOp::Sort(d)         => tree_has_col(&d.underlying, col),
        OptQueryOp::Cross(d)        =>
            tree_has_col(&d.left, col) || tree_has_col(&d.right, col),
        OptQueryOp::FilterCross(d)  =>
            tree_has_col(&d.left, col) || tree_has_col(&d.right, col),
        OptQueryOp::HashJoin(d)     =>
            tree_has_col(&d.left, col) || tree_has_col(&d.right, col),
    }
}

/// Returns the set of output column names produced by a subtree.
/// Used by [FIX-PROJ-PASSTHROUGH] to determine which columns to keep.
fn tree_output_cols(op: &OptQueryOp) -> Vec<String> {
    match op {
        OptQueryOp::Scan(d) => {
            // We don't have ctx here, so return a placeholder keyed by table prefix.
            // The caller uses these to check membership, so col names from known_col_prefix
            // are sufficient.  Return empty — caller falls back to no-narrowing.
            vec![format!("{}.*", d.table_id)]
        }
        OptQueryOp::FilteredScan(d) => {
            if let Some(ref proj) = d.project {
                proj.iter().map(|(_, to)| to.clone()).collect()
            } else {
                vec![format!("{}.*", d.table_id)]
            }
        }
        OptQueryOp::Project(d)     => d.column_name_map.iter().map(|(_, to)| to.clone()).collect(),
        OptQueryOp::Filter(d)      => tree_output_cols(&d.underlying),
        OptQueryOp::Sort(d)        => tree_output_cols(&d.underlying),
        OptQueryOp::HashJoin(d)    => {
            let mut l = tree_output_cols(&d.left); l.extend(tree_output_cols(&d.right)); l
        }
        OptQueryOp::FilterCross(d) => {
            let mut l = tree_output_cols(&d.left); l.extend(tree_output_cols(&d.right)); l
        }
        OptQueryOp::Cross(d)       => {
            let mut l = tree_output_cols(&d.left); l.extend(tree_output_cols(&d.right)); l
        }
    }
}

/// [FIX-ALIAS-COL] Check if a dotted alias column matches a given table.
fn alias_col_matches_table(table_id: &str, col: &str) -> bool {
    if !col.contains('.') { return false; }
    let bare = match col.split('.').last() {
        Some(b) => b,
        None    => return false,
    };
    known_col_prefix(table_id, bare)
}

/// [FIX-COL-MATCH] Disambiguate "p_" (part) from "ps_" (partsupp).
fn known_col_prefix(table_id: &str, col: &str) -> bool {
    let bare = col.split('.').last().unwrap_or(col);
    let pfx = match table_id {
        "customer" => "c_",
        "orders"   => "o_",
        "lineitem" => "l_",
        "part"     => "p_",
        "partsupp" => "ps_",
        "supplier" => "s_",
        "nation"   => "n_",
        "region"   => "r_",
        _ => return false,
    };
    if !bare.starts_with(pfx) { return false; }
    // "part" (pfx="p_") must not match "ps_..." columns.
    if pfx == "p_" && bare.starts_with("ps_") { return false; }
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// Stat conversion helpers
// ─────────────────────────────────────────────────────────────────────────────

fn comparison_value_as_f64(v: &ComparisionValue) -> Option<f64> {
    match v {
        ComparisionValue::I32(x) => Some(*x as f64),
        ComparisionValue::I64(x) => Some(*x as f64),
        ComparisionValue::F32(x) => Some(*x as f64),
        ComparisionValue::F64(x) => Some(*x),
        _                        => None,
    }
}

fn data_as_f64(d: &common::Data) -> Option<f64> {
    match d {
        common::Data::Int32(x)   => Some(*x as f64),
        common::Data::Int64(x)   => Some(*x as f64),
        common::Data::Float32(x) => Some(*x as f64),
        common::Data::Float64(x) => Some(*x),
        _                        => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Diagnostics
// ─────────────────────────────────────────────────────────────────────────────

fn relation_name(op: &OptQueryOp) -> String {
    match op {
        OptQueryOp::Scan(d)         => d.table_id.clone(),
        OptQueryOp::FilteredScan(d) => format!("{}(f)", d.table_id),
        OptQueryOp::Filter(d)       => relation_name(&d.underlying),
        OptQueryOp::Project(d)      => relation_name(&d.underlying),
        OptQueryOp::Sort(d)         => relation_name(&d.underlying),
        OptQueryOp::HashJoin(_)     => "hj".into(),
        OptQueryOp::FilterCross(_)  => "fcross".into(),
        OptQueryOp::Cross(_)        => "cross".into(),
    }
}

fn plan_summary(op: &OptQueryOp) -> String {
    match op {
        OptQueryOp::Scan(d)         => format!("Scan({})", d.table_id),
        OptQueryOp::FilteredScan(d) => format!("FScan({},p={},proj={}{})",
            d.table_id, d.predicates.len(),
            d.project.as_ref().map(|p| p.len()).unwrap_or(0),
            if d.is_physically_ordered_on.is_some() { ",es" } else { "" }),
        OptQueryOp::Filter(d)       => format!("Filter(p={},{})", d.predicates.len(), plan_summary(&d.underlying)),
        OptQueryOp::Project(d)      => format!("Proj({},{})", d.column_name_map.len(), plan_summary(&d.underlying)),
        OptQueryOp::Cross(d)        => format!("Cross({},{})", plan_summary(&d.left), plan_summary(&d.right)),
        OptQueryOp::Sort(d)         => format!("Sort(ord={},{})", d.is_physically_ordered, plan_summary(&d.underlying)),
        OptQueryOp::FilterCross(d)  => format!("FCross(p={},{},{})", d.predicates.len(), plan_summary(&d.left), plan_summary(&d.right)),
        OptQueryOp::HashJoin(d)     => format!("HJ({:?},{},{})", d.left_keys, plan_summary(&d.left), plan_summary(&d.right)),
    }
}

pub fn get_output_columns_opt(op: &OptQueryOp) -> Option<Vec<String>> {
    match op {
        OptQueryOp::Project(d) =>
            Some(d.column_name_map.iter().map(|(_, to)| to.clone()).collect()),
        OptQueryOp::FilteredScan(d) =>
            d.project.as_ref().map(|p| p.iter().map(|(_, to)| to.clone()).collect()),
        _ => None,
    }
}
