use datafusion_expr::LogicalPlan;

use super::group::Group;
use std::cell::RefCell;
use std::rc::Rc;

/// Get all possible trees for a given group.
pub fn get_all_possible_trees(group: Rc<RefCell<Group>>) -> Vec<String> {
    let mut output = Vec::new();

    for mexpr in group.borrow().equivalent_logical_mexprs.borrow().iter() {
        let op = mexpr.op();
        if let LogicalPlan::TableScan(table_scan) = &*op.borrow() {
            return vec![table_scan.table_name.to_string()];
        }

        let mut lists = Vec::new();
        for operand in mexpr.operands() {
            lists.push(get_all_possible_trees(Rc::clone(operand)));
        }

        for product in get_cartesian_product(&lists) {
            output.push(format!("({})", product));
        }
    }

    output
}

/// Get the Cartesian product of a list of lists.
pub fn get_cartesian_product(lists: &[Vec<String>]) -> Vec<String> {
    if lists.is_empty() {
        return vec![String::new()];
    }

    let first_list = &lists[0];
    let remaining_lists = &lists[1..];

    let mut result = Vec::new();
    for s in first_list {
        for t in get_cartesian_product(remaining_lists) {
            if t.is_empty() {
                result.push(s.clone());
            } else {
                result.push(format!("{} {}", s, t));
            }
        }
    }

    result
}

/// Get the count of all possible trees for a given group.
pub fn get_all_possible_trees_count(group: Rc<RefCell<Group>>) -> u64 {
    let mut output = 0;

    // Verify that the group is explored and has no unexplored logical expressions
    assert!(
        group
            .borrow()
            .unexplored_equivalent_logical_mexprs
            .borrow()
            .is_empty()
    );
    assert!(group.borrow().explored);

    for mexpr in group.borrow().equivalent_logical_mexprs.borrow().iter() {
        let op = mexpr.op();
        if let LogicalPlan::TableScan(_) = &*op.borrow() {
            return 1;
        }

        let mut tree_count = 1;
        for operand in mexpr.operands() {
            // Assuming the operator is multiplicative, e.g., InnerJoin
            tree_count *= get_all_possible_trees_count(Rc::clone(operand));
        }

        output += tree_count;
    }

    output
}
