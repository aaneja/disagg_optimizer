use super::mexpr::MExpr;
use super::sourcenode::SourceNode;
use std::collections::{VecDeque, HashSet}; // VecDeque for Queue, HashSet for Set
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug)]
pub struct Group {
    pub explored: bool,
    pub rowcount: i32, // replace with real stats object
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
            rowcount: 0,
            start_expression: Some(start_expression),
            cheapest_logical_expression: None,
            cheapest_physical_expression: None,
            unexplored_equivalent_logical_mexprs: RefCell::new(VecDeque::new()), // Empty queue
            equivalent_logical_mexprs: RefCell::new(Vec::new()), // Empty vector
            physical_manifestations: RefCell::new(HashSet::new()), // Empty hash set
            source_node: None,
        }
    }

    pub fn get_source_node_group(id: String) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            explored: false,
            rowcount: 0,
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
        group.borrow_mut().unexplored_equivalent_logical_mexprs.borrow_mut().push_back(mexpr);

        group
    }

    pub fn get_group_hash(&self) -> u64 {
        self.start_expression.as_ref().map(|expr| expr.hash()).unwrap_or(0)
    }

    pub fn set_explored(&mut self, explored: bool) {
        self.explored = explored;
    }

    pub fn is_explored(&self) -> bool {
        self.explored
    }
}
