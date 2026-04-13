// query_optimiser.rs — query rewrite / optimisation pass
//
// Optimisations applied bottom-up:
//   1. Scalar predicate push-down into Cross children
//      Filter(preds, Cross(L, R)) splits preds:
//        - scalar preds referencing only L → pushed into L
//        - scalar preds referencing only R → pushed into R
//        - join preds (col = col)          → kept at join level
//      Result: FilterCross { join_preds, left, right }
//              where left/right may themselves be wrapped in Filter.
//
//   2. Equality join detection
//      If ALL join_preds are col=col equalities → HashJoin node
//      otherwise                                → FilterCross node (NLJ with filter)

use common::query::*;

// ─────────────────────────────────────────────────────────────────────────────
// Extended node types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OptFilterData {
    pub predicates: Vec<Predicate>,
    pub underlying: Box<OptQueryOp>,
}

/// NLJ with predicates applied inside the loop
#[derive(Debug, Clone)]
pub struct FilterCrossData {
    pub predicates: Vec<Predicate>,
    pub left:       Box<OptQueryOp>,
    pub right:      Box<OptQueryOp>,
}

/// Hash join: build hash table on smaller side, probe with larger side.
/// join_keys: pairs of (left_col, right_col) that must be equal.
/// Extra predicates (non-equality) are checked after hash lookup.
#[derive(Debug, Clone)]
pub struct HashJoinData {
    pub left_keys:  Vec<String>,   // columns from left side
    pub right_keys: Vec<String>,   // columns from right side (same order)
    pub extra_preds: Vec<Predicate>, // any remaining non-equality predicates
    pub left:       Box<OptQueryOp>,
    pub right:      Box<OptQueryOp>,
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
            match child {
                OptQueryOp::Cross(OptCrossData { left, right }) => {
                    fuse_filter_cross(predicates, left, right)
                }
                other => OptQueryOp::Filter(OptFilterData {
                    predicates,
                    underlying: Box::new(other),
                }),
            }
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
// Core optimisation: fuse Filter + Cross
// ─────────────────────────────────────────────────────────────────────────────

fn fuse_filter_cross(
    predicates: Vec<Predicate>,
    left:  Box<OptQueryOp>,
    right: Box<OptQueryOp>,
) -> OptQueryOp {
    let mut left_scalar:  Vec<Predicate> = Vec::new();
    let mut right_scalar: Vec<Predicate> = Vec::new();
    let mut join_preds:   Vec<Predicate> = Vec::new();

    for pred in predicates {
        match &pred.value {
            // col = col  →  potential join predicate
            ComparisionValue::Column(_) => {
                join_preds.push(pred);
            }
            // scalar value  →  push to whichever side owns the column
            _ => {
                if tree_has_col(&left, &pred.column_name) {
                    left_scalar.push(pred);
                } else if tree_has_col(&right, &pred.column_name) {
                    right_scalar.push(pred);
                } else {
                    // Can't determine side statically — keep at join level
                    join_preds.push(pred);
                }
            }
        }
    }

    eprintln!(
        "[OPT] fuse Filter+Cross: {} left-scalar, {} right-scalar, {} join",
        left_scalar.len(), right_scalar.len(), join_preds.len()
    );

    // Wrap sides with their scalar filters
    let left_opt = if left_scalar.is_empty() {
        left
    } else {
        Box::new(OptQueryOp::Filter(OptFilterData {
            predicates: left_scalar,
            underlying: left,
        }))
    };
    let right_opt = if right_scalar.is_empty() {
        right
    } else {
        Box::new(OptQueryOp::Filter(OptFilterData {
            predicates: right_scalar,
            underlying: right,
        }))
    };

    // Check if all join preds are col=col equalities → hash join
    let all_eq = join_preds.iter().all(|p| p.operator == ComparisionOperator::EQ);
    let all_col_col = join_preds.iter().all(|p| matches!(&p.value, ComparisionValue::Column(_)));

    if !join_preds.is_empty() && all_eq && all_col_col {
        let mut left_keys:  Vec<String> = Vec::new();
        let mut right_keys: Vec<String> = Vec::new();
        let mut extra:      Vec<Predicate> = Vec::new();

        for pred in &join_preds {
            if let ComparisionValue::Column(ref rhs_col) = pred.value {
                // Figure out which col belongs to which side
                let lhs = pred.column_name.clone();
                let rhs = rhs_col.clone();
                if tree_has_col(&left_opt, &lhs) && tree_has_col(&right_opt, &rhs) {
                    left_keys.push(lhs);
                    right_keys.push(rhs);
                } else if tree_has_col(&left_opt, &rhs) && tree_has_col(&right_opt, &lhs) {
                    left_keys.push(rhs);
                    right_keys.push(lhs);
                } else {
                    // Can't resolve sides — fall back to extra pred
                    extra.push(pred.clone());
                }
            }
        }

        if !left_keys.is_empty() {
            eprintln!("[OPT] → HashJoin on {:?} = {:?}", left_keys, right_keys);
            return OptQueryOp::HashJoin(HashJoinData {
                left_keys,
                right_keys,
                extra_preds: extra,
                left:  left_opt,
                right: right_opt,
            });
        }
    }

    // Fall back to NLJ with filter inside loop
    eprintln!("[OPT] → FilterCross (NLJ)");
    OptQueryOp::FilterCross(FilterCrossData {
        predicates: join_preds,
        left:  left_opt,
        right: right_opt,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: does this subtree produce a column with this name?
// We match both "col" and "table.col" forms.
// ─────────────────────────────────────────────────────────────────────────────

fn tree_has_col(op: &OptQueryOp, col: &str) -> bool {
    match op {
        OptQueryOp::Scan(d) => {
            // Column names in rows will be "table_id.col_name"
            // At optimise time we only have the table_id, not the schema.
            // So we match if col starts with "table_id." or if the bare col
            // starts with a known prefix for that table.
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

/// Heuristic: TPC-H column prefixes map to tables.
/// This avoids needing ctx (the schema) in the optimiser.
fn table_likely_has_col(table_id: &str, col: &str) -> bool {
    // Each TPC-H table has a unique column prefix
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