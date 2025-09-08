use super::rulematcher::RuleMatcher;
use super::group::Group;
use super::mexpr::MExpr;
use super::operator::Operator;
use std::rc::Rc;
use std::cell::RefCell;
use ahash::AHashMap; // Using ahash for better performance
use datafusion_expr::LogicalPlan;

#[derive(Debug)]
pub struct Cascades {
    // Using AHashMap (high-performance HashMap) with u32 keys (hash values) and Arc<Group> values
    // Arc provides shared ownership similar to Java's reference semantics
    memo: AHashMap<u64, Rc<RefCell<Group>>>, // Updated to use u64 for hash keys
    rulematcher: RuleMatcher,
}

impl Cascades {
    pub fn new() -> Self {
        let memo = AHashMap::new();
        let rulematcher = RuleMatcher::new();

        Self {
            memo,
            rulematcher,
        }
    }

    pub fn optimize(&mut self, root_group: Rc<RefCell<Group>>) {
        self.rulematcher.explore(root_group, &mut self.memo); // Updated to pass AHashMap<u64, _>
    }

    /// Generates groups for a join nodes string as a right deep tree
    /// Example of a joinNodes string : "1234" => 4 source nodes with ids 1, 2, 3 and 4
    fn gen_groups(&mut self, join_nodes: &str) -> Rc<RefCell<Group>> {
        let plan_mexpr = if join_nodes.len() > 1 {
            let left = self.gen_groups(&join_nodes[0..1]);
            let right = self.gen_groups(&join_nodes[1..]);
            MExpr::build_with(Operator::InnerJoin, vec![left, right])
        } else {
            MExpr::build_with(Operator::Source, vec![Group::get_source_node_group(join_nodes.to_string())])
        };

        self.gen_or_get_from_memo(plan_mexpr)
    }

    fn gen_or_get_from_memo(&mut self, plan_mexpr: MExpr) -> Rc<RefCell<Group>> {
        let hash = plan_mexpr.hash();

        // Check if already exists in memo (HashMap lookup)
        if let Some(group) = self.memo.get(&hash) {
            return Rc::clone(group);
        }

        // Create new group and add to memo
        let new_group = Group::from_mexpr(plan_mexpr);
        self.memo.insert(hash, Rc::clone(&new_group));
        new_group
    }

    pub fn seed_memo(&mut self, join_nodes: &str) -> Rc<RefCell<Group>> {
        let root = self.gen_groups(join_nodes);
        // Util.printPlan(root.getStartExpression());
        // self.print_memo();
        root
    }

    pub fn print_memo(&self) {
        println!("Memo :");
        for (key, value) in &self.memo {
            let sources = if let Some(ref start_expr) = value.borrow().start_expression {
                start_expr.get_sorted_sources()
            } else if let Some(ref source_node) = value.borrow().source_node {
                source_node.node_id.clone()
            } else {
                "Unknown".to_string()
            };

            println!("{} : [{:p}, {}]",
                key,
                Rc::as_ptr(value),
                sources
            );
        }
    }

    pub fn get_unique_groups_in_memo(&self) -> Vec<Rc<RefCell<Group>>> {
        // Converting HashMap values to Vec, equivalent to ImmutableSet.copyOf() in Java
        self.memo.values().cloned().collect()
    }

    pub fn print_memo_stats(&self) {
        // Note: Rust doesn't have direct equivalent to Java's ClassLayout.parseInstance()
        // This would require external crates like memoffset or manual memory layout analysis
        println!("Memo contains {} entries", self.memo.len());
        println!("Memo capacity: {}", self.memo.capacity());
    }

    // Getter for memo (equivalent to @Getter annotation in Java)
    pub fn get_memo(&self) -> &AHashMap<u64, Rc<RefCell<Group>>> {
        &self.memo
    }

    pub fn gen_group_logical_plan(&mut self, plan: Rc<RefCell<LogicalPlan>>) -> Rc<RefCell<Group>> {
        let operands: Vec<Rc<RefCell<Group>>> = match &*plan.borrow() {
            LogicalPlan::Projection(proj) => vec![
                self.gen_group_logical_plan(Rc::new(RefCell::new(proj.input.as_ref().clone())))
            ],
            LogicalPlan::Filter(filter) => vec![
                self.gen_group_logical_plan(Rc::new(RefCell::new(filter.input.as_ref().clone())))
            ],
            LogicalPlan::Join(join) => vec![
                self.gen_group_logical_plan(Rc::new(RefCell::new(join.left.as_ref().clone()))),
                self.gen_group_logical_plan(Rc::new(RefCell::new(join.right.as_ref().clone()))),
            ],
            LogicalPlan::TableScan(_) => vec![],
            _ => unimplemented!("Support for this LogicalPlan variant is not yet implemented"),
        };

        let mexpr = MExpr::build_with_node(plan, operands);
        self.gen_or_get_from_memo(mexpr)
    }
}
