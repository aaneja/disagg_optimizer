use super::operator::Operator;
use super::group::Group;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::cell::RefCell;
use xxhash_rust::xxh3::Xxh3; // Import Xxh3 struct for direct usage
use datafusion_expr::LogicalPlan;

#[derive(Debug, Clone)]
pub struct MExpr {
    hash: u64, // Changed back to u64 to retain precision
    cost: i64,
    op: Rc<RefCell<LogicalPlan>>, // Store LogicalPlan node directly
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
            cost: 0, // Default cost
            op: Rc::new(RefCell::new(LogicalPlan::default())), // Placeholder, since this method would be replaced anyhow
            operands,
            canonicalized: hash.to_string(),
        }
    }

    pub fn build_with_node(node: Rc<RefCell<LogicalPlan>>, operands: Vec<Rc<RefCell<Group>>>) -> Self {
        let mut hasher = Xxh3::new(); // Create a new Xxh3 hasher
        node.borrow().hash(&mut hasher); // Hash the LogicalPlan node directly, this can be improved by taking only relevant struct fields and ignoring the parent

        // Commented below since we rely on the LogicalPlan's hash
        // for operand in &operands {
        //     if let Some(ref source_node) = operand.borrow().source_node {
        //         hasher.update(source_node.node_id.as_bytes());
        //     } else if operands.len() > 0 {
        //         hasher.update(operand.borrow().get_group_hash().to_le_bytes().as_ref());
        //     }
        // }

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

    // Getters
    pub fn hash(&self) -> u64 { self.hash } // Revert return type to u64 to match the field
    pub fn cost(&self) -> i64 { self.cost }
    pub fn op(&self) -> Rc<RefCell<LogicalPlan>> { Rc::clone(&self.op) }
    pub fn operands(&self) -> &Vec<Rc<RefCell<Group>>> { &self.operands }
    pub fn canonicalized(&self) -> &str { &self.canonicalized }
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
