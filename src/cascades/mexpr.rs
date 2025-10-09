use crate::cascades::constants::{
    DEFAULT_ROW_COUNT, FILTER_COST_PER_ROW, JOIN_COST_PER_ROW, PROJECT_COST_PER_ROW,
};

use super::group::Group;
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::{Expr, LogicalPlan};
use lazy_static::lazy_static;
use log::debug;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use xxhash_rust::xxh3::Xxh3;

#[derive(Debug, Clone)]
pub struct MExpr {
    hash: u64,
    cost: f64,
    row_count: u64,
    op: Rc<RefCell<LogicalPlan>>,      // Store LogicalPlan node directly
    operands: Vec<Rc<RefCell<Group>>>, // Using Rc and RefCell for shared ownership and mutability
    canonicalized: String,
}

impl MExpr {
    pub fn build_with_node(
        node: Rc<RefCell<LogicalPlan>>,
        operands: Vec<Rc<RefCell<Group>>>,
    ) -> Self {
        let mut hasher = Xxh3::new(); // Create a new Xxh3 hasher
        let mut row_count = DEFAULT_ROW_COUNT; // Default row count, need to improve this
        let mut cost = 0.0;
        let mut operand_row_counts: Vec<u64> = Vec::new();

        // Hash operands first, this way we can extract their properties
        for operand in &operands {
            // All nodes, including the TableScan node will be a group
            hasher.update(operand.borrow().get_group_hash().to_le_bytes().as_ref());
            operand_row_counts.push(operand.borrow().get_group_row_count());
        }

        // Hash the operator type and its specific properties, excluding children
        match node.borrow().clone() {
            LogicalPlan::Projection(proj) => {
                proj.schema.hash(&mut hasher);
                proj.expr.hash(&mut hasher);

                row_count = operand_row_counts
                    .first()
                    .cloned()
                    .unwrap_or(DEFAULT_ROW_COUNT);
                cost = PROJECT_COST_PER_ROW * row_count as f64; // Assume projection has a small cost
            }
            LogicalPlan::Filter(filter) => {
                filter.predicate.hash(&mut hasher);

                row_count = (0.10
                    * operand_row_counts
                        .first()
                        .cloned()
                        .unwrap_or(DEFAULT_ROW_COUNT) as f64) as u64; // Assume filter reduces rows by 90%
                cost = FILTER_COST_PER_ROW * row_count as f64;
            }
            LogicalPlan::Join(join) => {
                join.join_type.hash(&mut hasher);
                // TODO : We need to fix the hashing for the ON clauses, so that a join node with [a = b] and [b = a] hash the same
                // TODO : Because rulematcher.split_eq_and_noneq_join_predicate is not correctly generating equality inferences
                // TODO : We are seeing CROSS JOINs while these would have been correctly generated as Inner Joins with ON clauses
                // join.on.hash(&mut hasher);
                join.filter.hash(&mut hasher);
                join.join_constraint.hash(&mut hasher);

                //debug!("Join plan: {:?}", join.on);

                // Simplistic cost model for now , we use pre canned selectivities
                // We will later add NDV stats based estimation
                let selectivity = Self::get_join_selectivity(&join.on);
                debug!(
                    "Estimated selectivity for join {:?} is {}",
                    join.on, selectivity
                );
                if selectivity.is_finite() {
                    row_count = (selectivity * operand_row_counts.iter().product::<u64>() as f64) as u64;
                    cost = JOIN_COST_PER_ROW * row_count as f64;
                } else {
                    // Cross join
                    row_count = operand_row_counts.iter().product();
                    cost = JOIN_COST_PER_ROW * row_count as f64;
                }
            }
            LogicalPlan::TableScan(ts) => {
                ts.hash(&mut hasher);
                row_count = ts.fetch.unwrap_or(DEFAULT_ROW_COUNT.try_into().unwrap()) as u64;
                cost = row_count as f64;
            }
            _ => { /* Fix the other nodes similarly*/ }
        };

        let hash = hasher.digest();

        Self {
            hash,
            cost: cost,
            row_count: row_count,
            op: node,
            operands,
            canonicalized: hash.to_string(),
        }
    }

    pub fn get_schema(&self) -> Option<Arc<DFSchema>> {
        let mut current_node = self.op.borrow().clone();

        loop {
            match current_node {
                LogicalPlan::Projection(proj) => return Some(proj.schema),
                LogicalPlan::Filter(filter) => current_node = (*filter.input).clone(),
                LogicalPlan::Aggregate(agg) => return Some(agg.schema),
                LogicalPlan::Join(join) => return Some(join.schema),
                LogicalPlan::Sort(sort) => current_node = (*sort.input).clone(),
                LogicalPlan::TableScan(scan) => return Some(scan.projected_schema.clone()),
                LogicalPlan::Limit(limit) => current_node = (*limit.input).clone(),
                LogicalPlan::Union(union) => {
                    if let Some(first_input) = union.inputs.first() {
                        current_node = (**first_input).clone();
                    } else {
                        return None;
                    }
                }
                LogicalPlan::EmptyRelation(empty) => return Some(empty.schema.clone()),
                _ => return None, // Handle other cases or stop if schema is not found
            }
        }
    }

    // Getters
    pub fn hash(&self) -> u64 {
        self.hash
    } // Revert return type to u64 to match the field
    pub fn cost(&self) -> f64 {
        self.cost
    }
    pub fn op(&self) -> Rc<RefCell<LogicalPlan>> {
        Rc::clone(&self.op)
    }
    pub fn operands(&self) -> &Vec<Rc<RefCell<Group>>> {
        &self.operands
    }
    pub fn canonicalized(&self) -> &str {
        &self.canonicalized
    }
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn get_join_selectivity(join_on: &[(Expr, Expr)]) -> f64 {
        if let Some((left_expr, right_expr)) = join_on.first() {
            let mut left_table = None;
            let mut right_table = None;

            // Parse the left expression to determine the table used
            if let Expr::Column(column) = left_expr {
                if let Some(table_ref) = &column.relation {
                    // debug!("Left Table used in join: {:?}", table_ref);
                    left_table = Some(table_ref.to_string());
                } else {
                    debug!("Left Table reference is not available");
                }
            } else {
                debug!("Left expression is not a column");
            }

            // Parse the right expression to determine the table used
            if let Expr::Column(column) = right_expr {
                if let Some(table_ref) = &column.relation {
                    // debug!("Right Table used in join: {:?}", table_ref);
                    right_table = Some(table_ref.to_string());
                } else {
                    debug!("Right Table reference is not available");
                }
            } else {
                debug!("Right expression is not a column");
            }

            // Lookup selectivity if both tables are resolved
            if let (Some(left), Some(right)) = (left_table, right_table) {
                if let Some(&selectivity) = SELECTIVITY_MAP.get(&(left.as_str(), right.as_str())) {
                    return selectivity;
                } else if let Some(&selectivity) =
                    SELECTIVITY_MAP.get(&(right.as_str(), left.as_str()))
                {
                    return selectivity;
                } else {
                    debug!("Selectivity not found for tables: ({}, {})", left, right);
                }
            }
        } else {
            debug!("Join condition is empty");
        }

        // Cross join
        f64::INFINITY
    }
}

impl Hash for MExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl PartialEq for MExpr {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for MExpr {}

lazy_static! {
    pub static ref SELECTIVITY_MAP: HashMap<(&'static str, &'static str), f64> = {
        let mut map = HashMap::new();
        // Some example selectivities between tables to start off
        map.insert(("t1", "t2"), 0.001);
        map.insert(("t1", "t3"), 0.001);
        map.insert(("t1", "t4"), 0.001);
        map.insert(("t1", "t5"), 0.001);
        map.insert(("t2", "t3"), 0.01);
        map.insert(("t2", "t4"), 0.01);
        map.insert(("t2", "t5"), 0.01);
        map.insert(("t3", "t4"), 0.0001);
        map.insert(("t3", "t5"), 0.0001);
        map.insert(("t4", "t5"), 0.1);
        map
    };
}
