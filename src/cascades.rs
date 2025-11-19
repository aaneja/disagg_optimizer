pub mod group;
pub mod mexpr;
pub mod rulematcher;
pub mod sourcenode;
pub mod operator;
pub mod util;
pub mod constants;
pub mod expression_utils;

use rulematcher::RuleMatcher;
use group::Group;
use mexpr::MExpr;
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
    pub fn default() -> Self {
        let memo = AHashMap::new();
        let rulematcher = RuleMatcher::default();

        Self {
            memo,
            rulematcher,
        }
    }

    pub fn optimize(&mut self, root_group: Rc<RefCell<Group>>) {
        self.rulematcher.explore(root_group, &mut self.memo); 
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

    pub fn print_memo(&self) {
        println!("Memo :");
        for (key, value) in &self.memo {
            let sources = if let Some(ref start_expr) = value.borrow().start_expression {
                format!("{} ", start_expr.op().borrow().display())
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
