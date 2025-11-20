use super::mexpr::MExpr;
use super::sourcenode::SourceNode;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::rc::Rc;

#[derive(Debug)]
pub struct Group {
    explored: bool,
    pub min_cost: f64, // For now, assuming that 0.0 => UNKNOWN cost
    pub start_expression: Option<MExpr>,
    pub cheapest_logical_expression: Option<MExpr>,
    pub cheapest_physical_expression: Option<MExpr>,

    // Using VecDeque as equivalent to Java's LinkedList/Queue
    pub unexplored_equivalent_logical_mexprs: RefCell<VecDeque<MExpr>>,

    // Using Vec as equivalent to Java's ArrayList
    pub equivalent_logical_mexprs: RefCell<Vec<MExpr>>,

    // Using HashSet as equivalent to Java's HashSet
    pub physical_manifestations: RefCell<HashSet<MExpr>>,

    // Using Option for Java's Optional
    pub source_node: Option<SourceNode>,
}

impl Group {
    pub fn new(start_expression: MExpr) -> Self {
        Self {
            explored: false,
            min_cost: 0.0,
            start_expression: Some(start_expression),
            cheapest_logical_expression: None,
            cheapest_physical_expression: None,
            unexplored_equivalent_logical_mexprs: RefCell::new(VecDeque::new()), // Empty queue
            equivalent_logical_mexprs: RefCell::new(Vec::new()),                 // Empty vector
            physical_manifestations: RefCell::new(HashSet::new()),               // Empty hash set
            source_node: None,
        }
    }

    pub fn from_mexpr(mexpr: MExpr) -> Rc<RefCell<Self>> {
        let group = Rc::new(RefCell::new(Self::new(mexpr.clone())));

        // Add to unexplored queue
        group
            .borrow_mut()
            .unexplored_equivalent_logical_mexprs
            .borrow_mut()
            .push_back(mexpr);

        group
    }

    pub fn get_group_hash(&self) -> u64 {
        self.start_expression
            .as_ref()
            .map(|expr| expr.hash())
            .unwrap_or(0)
    }

    pub fn get_group_row_count(&self) -> u64 {
        if !self.explored {
            log::debug!(
                "Group is not explored and we are using the default row count from start expression"
            );

            self.start_expression
                .as_ref()
                .map(|expr| expr.row_count())
                .unwrap_or(0);
        }

        self.cheapest_logical_expression
            .as_ref()
            .map(|expr| expr.row_count())
            .unwrap_or(0)
    }

    pub fn get_group_cost(&self) -> f64 {
        if !self.explored {
            log::debug!("Group is not explored and we are using the default cost of 0.0");
        }
        self.min_cost
    }

    pub fn set_explored(&mut self, explored: bool) {
        self.explored = explored;
        // Find the cheapest logical expression from equivalent_logical_mexprs
        self.equivalent_logical_mexprs
            .borrow()
            .iter()
            .for_each(|mexpr| {
                if let Some(ref cheapest) = self.cheapest_logical_expression {
                    if mexpr.cost() < cheapest.cost() {
                        self.cheapest_logical_expression = Some(mexpr.clone());
                    }
                } else {
                    self.cheapest_logical_expression = Some(mexpr.clone());
                }
            });

        self.min_cost = self
            .cheapest_logical_expression
            .as_ref()
            .map(|expr| expr.cost())
            .unwrap_or(0.0);
    }

    pub fn is_explored(&self) -> bool {
        self.explored
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::mexpr::MExpr;
    use crate::cascades::test_utils;
    use datafusion_common::DFSchema;
    use datafusion_expr::{EmptyRelation, LogicalPlan};
    use std::sync::Arc;

    #[test]
    fn test_empty_relation() {
        let logical_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        verify_row_count(logical_plan, 0, 0.0);
    }

    #[test]
    fn test_default() {
        verify_row_count(LogicalPlan::default(), 0, 0.0);
    }

    #[tokio::test]
    async fn test_simple_logical_plan() {
        let logical_plan = test_utils::generate_logical_plan(vec![100, 200, 30, 400]).await;
        let plan_string = test_utils::custom_print(&logical_plan).expect("unable to print");
        println!("Plan string {}", plan_string);
        verify_row_count(logical_plan, 0, 0.0);
    }

    fn verify_row_count(logical_plan: LogicalPlan, expected_row_count: u64, expected_cost: f64) {
        let mexpr = MExpr::build_with_node(Rc::new(RefCell::new(logical_plan)), vec![]);
        let mut group = Group::new(mexpr.clone());
        let row_count = group.get_group_row_count();
        println!("Group row count: {}", row_count);
        assert_eq!(row_count, expected_row_count);
        assert_eq!(group.get_group_cost(), expected_cost);
    }
}
