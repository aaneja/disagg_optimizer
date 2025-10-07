use super::mexpr::MExpr;
use super::sourcenode::SourceNode;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque}; // VecDeque for Queue, HashSet for Set
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

    pub fn get_source_node_group(id: String) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            explored: false,
            min_cost: 0.0,
            start_expression: None, // Source nodes don't have start expressions
            cheapest_logical_expression: None,
            cheapest_physical_expression: None,
            unexplored_equivalent_logical_mexprs: RefCell::new(VecDeque::new()),
            equivalent_logical_mexprs: RefCell::new(Vec::new()),
            physical_manifestations: RefCell::new(HashSet::new()),
            source_node: Some(SourceNode::new(id)),
        }))
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
        self.start_expression
            .as_ref()
            .map(|expr| expr.row_count())
            .unwrap_or(0)
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
            .unwrap_or(0.0) as f64; // Assuming cost is f64, convert to u64
    }

    pub fn is_explored(&self) -> bool {
        self.explored
    }
}
