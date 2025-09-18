use super::group::Group;
use super::operator::Operator;
use datafusion_common::{DFSchema};
use datafusion_expr::{LogicalPlan};
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use xxhash_rust::xxh3::Xxh3; 

#[derive(Debug, Clone)]
pub struct MExpr {
    hash: u64, // Changed back to u64 to retain precision
    cost: i64,
    op: Rc<RefCell<LogicalPlan>>,      // Store LogicalPlan node directly
    operands: Vec<Rc<RefCell<Group>>>, // Using Rc and RefCell for shared ownership and mutability
    canonicalized: String,
}

impl MExpr {
    pub fn build_with(op: Operator, operands: Vec<Rc<RefCell<Group>>>) -> Self {
        let mut hasher = Xxh3::new(); // Create a new Xxh3 hasher
        hasher.update((op as u8).to_le_bytes().as_ref());

        for operand in &operands {
            if let Some(ref source_node) = operand.borrow().source_node {
                hasher.update(source_node.node_id.as_bytes());
            } else if operands.len() > 0 {
                hasher.update(operand.borrow().get_group_hash().to_le_bytes().as_ref());
            }
        }

        let hash = hasher.digest();

        Self {
            hash,
            cost: 0,                                           // Default cost
            op: Rc::new(RefCell::new(LogicalPlan::default())), // Placeholder, since this method would be replaced anyhow
            operands,
            canonicalized: hash.to_string(),
        }
    }

    pub fn build_with_node(
        node: Rc<RefCell<LogicalPlan>>,
        operands: Vec<Rc<RefCell<Group>>>,
    ) -> Self {
        let mut hasher = Xxh3::new(); // Create a new Xxh3 hasher

        // Hash the operator type and its specific properties, excluding children
        match node.borrow().clone() {
            LogicalPlan::Projection(proj) => {
                proj.schema.hash(&mut hasher);
                proj.expr.hash(&mut hasher);
            }
            LogicalPlan::Filter(filter) => {
                filter.predicate.hash(&mut hasher);        
            }
            LogicalPlan::Join(join) => {
                join.join_type.hash(&mut hasher);
                // join.on.hash(&mut hasher); // TODO : We need to fix the hashing for the ON clauses, so that a join node with [a = b] and [b = a] hash the same
                join.filter.hash(&mut hasher);
                join.join_constraint.hash(&mut hasher);
            }
            LogicalPlan::TableScan(ts) => {
                ts.hash(&mut hasher); 
            }
            _ => { /* Fix the other nodes similarly*/ }
        };

        for operand in &operands {
            // All nodes, including the TableScan node will be a group
             hasher.update(operand.borrow().get_group_hash().to_le_bytes().as_ref());
        }

        let hash = hasher.digest();

        Self {
            hash,
            cost: 0, // Default cost
            op: node,
            operands,
            canonicalized: hash.to_string(),
        }
    }

    pub fn get_sorted_sources(&self) -> String {
        let sources = self.get_sources();
        let mut chars: Vec<char> = sources.chars().collect();
        chars.sort(); // Sorting lexicographically like Java's Arrays.sort
        chars.into_iter().collect()
    }

    pub fn get_sources(&self) -> String {
        let mut result = String::new();
        for operand in &self.operands {
            if let Some(ref source_node) = operand.borrow().source_node {
                result.push_str(&source_node.node_id);
            } else {
                // Use a placeholder to avoid infinite recursion
                result.push('X');
            }
        }
        result
    }

    pub fn get_full_string(&self) -> String {
        let mut operand_strings = Vec::new();
        for operand in &self.operands {
            if let Some(ref source_node) = operand.borrow().source_node {
                return source_node.node_id.clone();
            }
            operand_strings.push(format!("Group_{:p}", Rc::as_ptr(operand)));
        }

        format!("{:?} ( {} )", self.op, operand_strings.join(", "))
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
    pub fn cost(&self) -> i64 {
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
