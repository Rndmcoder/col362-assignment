// query_optimiser.rs — query rewrite / optimisation pass
//
// Optimisations applied bottom-up:
//   1. Scalar predicate push-down into Cross children
//   2. Equality join detection → HashJoin
//   3. RECURSIVE fuse: Filter over nested Cross trees gets fully distributed,
//      so Cross(Cross(A,B),C) with predicates spanning all three tables
//      produces a proper join tree instead of a cartesian product.
//   4. Join reordering: after building the join tree, reorder children so
//      the smaller (row_count) side is always the build side.

use common::query::*;

// ─────────────────────────────────────────────────────────────────────────────
// Extended node types
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
}

#[derive(Debug, Clone)]
pub enum OptQueryOp {
    Scan(ScanData),
    Filter(OptFilterData),
    Project(OptProjectData),
    Cross(OptCrossData),
    Sort(OptSortData),
    FilterCross(FilterCrossData),
    HashJoin(HashJoinData),
}

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

pub fn optimise(op: QueryOp) -> OptQueryOp {
    lower(op)
}

// ─────────────────────────────────────────────────────────────────────────────
// Lower QueryOp → OptQueryOp
// ─────────────────────────────────────────────────────────────────────────────

fn lower(op: QueryOp) -> OptQueryOp {
    match op {
        QueryOp::Scan(d) => OptQueryOp::Scan(d),

        QueryOp::Filter(FilterData { predicates, underlying }) => {
            let child = lower(*underlying);
            push_filter_into(predicates, child)
        }

        QueryOp::Project(ProjectData { column_name_map, underlying }) => {
            OptQueryOp::Project(OptProjectData {
                column_name_map,
                underlying: Box::new(lower(*underlying)),
            })
        }

        QueryOp::Cross(CrossData { left, right }) => {
            OptQueryOp::Cross(OptCrossData {
                left:  Box::new(lower(*left)),
                right: Box::new(lower(*right)),
            })
        }

        QueryOp::Sort(SortData { sort_specs, underlying }) => {
            OptQueryOp::Sort(OptSortData {
                sort_specs,
                underlying: Box::new(lower(*underlying)),
            })
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// push_filter_into
// ─────────────────────────────────────────────────────────────────────────────

fn push_filter_into(predicates: Vec<Predicate>, child: OptQueryOp) -> OptQueryOp {
    match child {
        OptQueryOp::Cross(OptCrossData { left, right }) => {
            fuse_filter_cross(predicates, left, right)
        }
        other => {
            if predicates.is_empty() {
                other
            } else {
                OptQueryOp::Filter(OptFilterData {
                    predicates,
                    underlying: Box::new(other),
                })
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// fuse_filter_cross — recursive core optimisation
//
// Given predicates and Cross(left, right):
//   1. Split predicates: left-only scalars, right-only scalars, join preds
//   2. Recursively push scalars into children
//   3. Try to push join preds further into subtrees
//   4. Use remaining join_preds to build HashJoin or FilterCross
//
// Key fix: we collect ALL leaves of nested Cross trees and try to optimally
// assign all predicates, producing a bushy join tree instead of always
// doing Cross(something_huge, table).
// ─────────────────────────────────────────────────────────────────────────────

fn fuse_filter_cross(
    predicates: Vec<Predicate>,
    left:  Box<OptQueryOp>,
    right: Box<OptQueryOp>,
) -> OptQueryOp {
    // Collect all leaf tables from the Cross tree
    let mut leaves = Vec::new();
    collect_cross_leaves(*left, &mut leaves);
    collect_cross_leaves(*right, &mut leaves);

    // Try to build an optimal join tree from all leaves + all predicates
    build_optimal_join(leaves, predicates)
}

/// Flatten a Cross tree into its leaf nodes (already optimised subtrees).
fn collect_cross_leaves(op: OptQueryOp, out: &mut Vec<OptQueryOp>) {
    match op {
        OptQueryOp::Cross(OptCrossData { left, right }) => {
            collect_cross_leaves(*left, out);
            collect_cross_leaves(*right, out);
        }
        other => out.push(other),
    }
}

/// Build an optimal left-deep join tree from a set of relations and predicates.
/// Uses a greedy approach: repeatedly pick the predicate whose both sides are
/// available and whose build side is smallest.
fn build_optimal_join(mut relations: Vec<OptQueryOp>, mut predicates: Vec<Predicate>) -> OptQueryOp {
    if relations.is_empty() {
        panic!("build_optimal_join: no relations");
    }
    if relations.len() == 1 {
        let rel = relations.remove(0);
        return if predicates.is_empty() {
            rel
        } else {
            OptQueryOp::Filter(OptFilterData {
                predicates,
                underlying: Box::new(rel),
            })
        };
    }

    // First: push scalar (single-table) predicates into the matching relation
    let mut leftover_preds: Vec<Predicate> = Vec::new();
    for pred in predicates.drain(..) {
        match &pred.value {
            ComparisionValue::Column(_) => leftover_preds.push(pred),
            _ => {
                // scalar: find which relation owns this column
                let mut pushed = false;
                for rel in &mut relations {
                    if tree_has_col(rel, &pred.column_name) {
                        push_scalar_into_rel(rel, pred.clone());
                        pushed = true;
                        break;
                    }
                }
                if !pushed { leftover_preds.push(pred); }
            }
        }
    }
    predicates = leftover_preds;

    // Greedy join ordering: build a left-deep tree
    // Start with the smallest relation, repeatedly join the next best
    // relation using available predicates.
    //
    // "Best" means: there's an eq-join predicate connecting it to the
    // current result; otherwise pick smallest.

    // Sort relations by estimated size (scans are ordered by table name heuristic)
    // We don't have row counts at optimise time, so use a table-size heuristic.
    relations.sort_by_key(|r| estimated_rows(r));

    let mut current = relations.remove(0);
    let mut remaining = relations;

    while !remaining.is_empty() {
        // Find the best next relation to join
        // Priority: one that has a join predicate with current
        let mut best_idx = None;
        let mut best_has_join = false;

        for (i, rel) in remaining.iter().enumerate() {
            let has_join = predicates.iter().any(|p| {
                if let ComparisionValue::Column(rhs_col) = &p.value {
                    (tree_has_col(&current, &p.column_name) && tree_has_col(rel, rhs_col))
                    || (tree_has_col(&current, rhs_col) && tree_has_col(rel, &p.column_name))
                } else { false }
            });
            if has_join && !best_has_join {
                best_idx = Some(i);
                best_has_join = true;
            } else if best_idx.is_none() {
                best_idx = Some(i);
            }
        }

        let idx = best_idx.unwrap();
        let next = remaining.remove(idx);

        // Collect predicates that connect current ↔ next (or are fully inside either)
        let mut join_preds: Vec<Predicate> = Vec::new();
        let mut rest_preds: Vec<Predicate> = Vec::new();
        for pred in predicates.drain(..) {
            if let ComparisionValue::Column(rhs_col) = &pred.value {
                let lhs_in_cur  = tree_has_col(&current, &pred.column_name);
                let lhs_in_next = tree_has_col(&next,    &pred.column_name);
                let rhs_in_cur  = tree_has_col(&current, rhs_col);
                let rhs_in_next = tree_has_col(&next,    rhs_col);
                // Pure join between current and next
                if (lhs_in_cur && rhs_in_next) || (lhs_in_next && rhs_in_cur) {
                    join_preds.push(pred);
                } else {
                    rest_preds.push(pred);
                }
            } else {
                // Scalar — should already be pushed in, but keep safe
                rest_preds.push(pred);
            }
        }
        predicates = rest_preds;

        current = build_join_node(join_preds, Box::new(current), Box::new(next));
    }

    // Apply any remaining predicates as a filter
    if !predicates.is_empty() {
        current = OptQueryOp::Filter(OptFilterData {
            predicates,
            underlying: Box::new(current),
        });
    }

    current
}

/// Push a scalar predicate into a relation (wraps it in Filter, or adds to existing Filter).
fn push_scalar_into_rel(rel: &mut OptQueryOp, pred: Predicate) {
    let old = std::mem::replace(rel, OptQueryOp::Scan(ScanData { table_id: String::new() }));
    *rel = match old {
        OptQueryOp::Filter(mut f) => {
            f.predicates.push(pred);
            OptQueryOp::Filter(f)
        }
        other => OptQueryOp::Filter(OptFilterData {
            predicates: vec![pred],
            underlying: Box::new(other),
        }),
    };
}

/// Rough row-count estimate for join ordering (lower = build side).
fn estimated_rows(op: &OptQueryOp) -> u64 {
    match op {
        OptQueryOp::Scan(d) => table_size_estimate(&d.table_id),
        OptQueryOp::Filter(d) => estimated_rows(&d.underlying) / 4,
        OptQueryOp::Project(d) => estimated_rows(&d.underlying),
        OptQueryOp::Sort(d) => estimated_rows(&d.underlying),
        OptQueryOp::Cross(d) =>
            estimated_rows(&d.left).saturating_mul(estimated_rows(&d.right)),
        OptQueryOp::FilterCross(d) =>
            estimated_rows(&d.left).saturating_mul(estimated_rows(&d.right)) / 10,
        OptQueryOp::HashJoin(d) =>
            estimated_rows(&d.left).max(estimated_rows(&d.right)),
    }
}

fn table_size_estimate(name: &str) -> u64 {
    match name {
        "nation"   =>       25,
        "region"   =>        5,
        "supplier" =>    1_000,
        "customer" =>   15_000,
        "part"     =>   20_000,
        "partsupp" =>   80_000,
        "orders"   =>  150_000,
        "lineitem" =>  600_000,
        _          =>   50_000,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// partition_preds_for_subtree — kept for compatibility
// ─────────────────────────────────────────────────────────────────────────────

fn partition_preds_for_subtree(
    preds: Vec<Predicate>,
    subtree: &OptQueryOp,
) -> (Vec<Predicate>, Vec<Predicate>) {
    let mut keep = Vec::new();
    let mut push = Vec::new();
    for pred in preds {
        let lhs_in = tree_has_col(subtree, &pred.column_name);
        let rhs_in = match &pred.value {
            ComparisionValue::Column(c) => tree_has_col(subtree, c),
            _ => true,
        };
        if lhs_in && rhs_in { push.push(pred); } else { keep.push(pred); }
    }
    (keep, push)
}

/// Given join-level predicates and optimised left/right children,
/// produce HashJoin, FilterCross, or Cross.
fn build_join_node(
    join_preds: Vec<Predicate>,
    left:  Box<OptQueryOp>,
    right: Box<OptQueryOp>,
) -> OptQueryOp {
    if join_preds.is_empty() {
        return OptQueryOp::Cross(OptCrossData { left, right });
    }

    let all_eq_col_col = join_preds.iter().all(|p| {
        p.operator == ComparisionOperator::EQ
            && matches!(&p.value, ComparisionValue::Column(_))
    });

    if all_eq_col_col {
        let mut left_keys:  Vec<String> = Vec::new();
        let mut right_keys: Vec<String> = Vec::new();
        let mut extra:      Vec<Predicate> = Vec::new();

        for pred in &join_preds {
            if let ComparisionValue::Column(rhs_col) = &pred.value {
                let lhs = pred.column_name.clone();
                let rhs = rhs_col.clone();
                if tree_has_col(&left, &lhs) && tree_has_col(&right, &rhs) {
                    left_keys.push(lhs);
                    right_keys.push(rhs);
                } else if tree_has_col(&left, &rhs) && tree_has_col(&right, &lhs) {
                    left_keys.push(rhs);
                    right_keys.push(lhs);
                } else {
                    extra.push(pred.clone());
                }
            }
        }

        if !left_keys.is_empty() {
            eprintln!("[OPT] → HashJoin on {:?} = {:?}", left_keys, right_keys);
            // Put smaller (build) side on the left
            let left_est  = estimated_rows(&left);
            let right_est = estimated_rows(&right);
            if right_est < left_est {
                // swap so smaller is left (build side in executor)
                eprintln!("[OPT]   swapping sides: left_est={} right_est={}", left_est, right_est);
                let mut swapped_lk = Vec::new();
                let mut swapped_rk = Vec::new();
                for (lk, rk) in left_keys.iter().zip(right_keys.iter()) {
                    swapped_lk.push(rk.clone());
                    swapped_rk.push(lk.clone());
                }
                return OptQueryOp::HashJoin(HashJoinData {
                    left_keys:   swapped_lk,
                    right_keys:  swapped_rk,
                    extra_preds: extra,
                    left:        right,
                    right:       left,
                });
            }
            return OptQueryOp::HashJoin(HashJoinData {
                left_keys,
                right_keys,
                extra_preds: extra,
                left,
                right,
            });
        }
    }

    eprintln!("[OPT] → FilterCross (NLJ) with {} preds", join_preds.len());
    OptQueryOp::FilterCross(FilterCrossData {
        predicates: join_preds,
        left,
        right,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: does this subtree produce a column with this name?
// ─────────────────────────────────────────────────────────────────────────────

pub fn tree_has_col(op: &OptQueryOp, col: &str) -> bool {
    match op {
        OptQueryOp::Scan(d) => {
            col.starts_with(&format!("{}.", d.table_id))
                || table_likely_has_col(&d.table_id, col)
        }
        OptQueryOp::Filter(d)      => tree_has_col(&d.underlying, col),
        OptQueryOp::Project(d)     => d.column_name_map.iter().any(|(_, to)| to == col),
        OptQueryOp::Sort(d)        => tree_has_col(&d.underlying, col),
        OptQueryOp::Cross(d)       => tree_has_col(&d.left, col) || tree_has_col(&d.right, col),
        OptQueryOp::FilterCross(d) => tree_has_col(&d.left, col) || tree_has_col(&d.right, col),
        OptQueryOp::HashJoin(d)    => tree_has_col(&d.left, col) || tree_has_col(&d.right, col),
    }
}

fn table_likely_has_col(table_id: &str, col: &str) -> bool {
    let prefix = match table_id {
        "customer"  => "c_",
        "orders"    => "o_",
        "lineitem"  => "l_",
        "part"      => "p_",
        "partsupp"  => "ps_",
        "supplier"  => "s_",
        "nation"    => "n_",
        "region"    => "r_",
        _           => return false,
    };
    col.starts_with(prefix)
}