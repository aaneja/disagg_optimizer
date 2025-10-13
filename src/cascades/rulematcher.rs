use super::group::Group;
use super::mexpr::MExpr;
use ahash::AHashMap;
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr_common::operator::Operator;

use datafusion::logical_expr::lit;
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{BinaryExpr, Expr};
use datafusion_expr::{Join, LogicalPlan};
use log::{debug};
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;
use crate::cascades::expression_utils::infer_equalities;

#[derive(Debug)]
pub struct RuleMatcher {
    // No fields needed as memo is passed as parameter
}

impl RuleMatcher {
    pub fn new() -> Self {
        Self {}
    }

    /// Check and apply rules to a Group.
    /// 1. Produce logically equivalent MExprs and generate new tasks for them
    /// 2. For every new Group for the generated MExpr, check if already have it explored in the memo, if so get the cheapest plan from it
    /// 3. Add any not previously explored groups to TasksQueue
    /// 4. Mark group as explored - note a cycle can occur where child tasks generate the parent ?? If so detect this cycle and fix it
    pub fn explore(
        &mut self,
        group: Rc<RefCell<Group>>,
        memo: &mut AHashMap<u64, Rc<RefCell<Group>>>,
    ) {
        if group.borrow().is_explored() {
            return; // Already explored
        }
        // Process all unexplored expressions
        while let Some(mut mexpr) = {
            let group_borrowed = group.borrow_mut();
            let mut unexplored = group_borrowed
                .unexplored_equivalent_logical_mexprs
                .borrow_mut();
            unexplored.pop_front()
        } {
            // TODO : Pass through upper and lower bound estimates as detailed in 
            // https://15721.courses.cs.cmu.edu/spring2023/papers/16-optimizer1/shapiro-ideas2001.pdf
            // before exploring this mexpr
            // If we already have a cheaper cost for this group, skip exploring this mexpr

            // For now, explore all children of this expression to completion
            // This is the 'traditional' Cascades implementation
            for operand in mexpr.operands() {
                self.explore(Rc::clone(operand), memo);
            }

            // Rule transformations can now match and bind against child groups correctly
            self.apply_transformation_rules(&group, &mexpr, memo);

            // This Expression is now explored
            mexpr.update_cost_and_rowcount(); // Fixup the cost and rowcount for this expression now that operands are explored
            group
                .borrow_mut()
                .equivalent_logical_mexprs
                .borrow_mut()
                .push(mexpr);
        }

        // Mark the group as fully explored; store the cheapest logical expression and its cost
        group.borrow_mut().set_explored(true);
    }

    fn apply_transformation_rules(
        &mut self,
        group: &Rc<RefCell<Group>>,
        mexpr: &MExpr,
        memo: &mut AHashMap<u64, Rc<RefCell<Group>>>,
    ) {
        // Replace below with a true rule matcher/binder/transformer
        // For now we simply apply join commutativity & associativity rules since we're only considering IJ reordering

        {
            let transformed = self.apply_join_commutativity(mexpr);
            self.add_new_mexprs(group, transformed, "Join Commutativity", memo);
        }

        {
            let transformed = self.apply_join_associativity(mexpr, memo);
            self.add_new_mexprs(group, transformed, "Join Associativity", memo);
        }
    }

    // (A ⋈ B) => (B ⋈ A)
    fn apply_join_commutativity(&self, mexpr: &MExpr) -> Vec<MExpr> {
        if let LogicalPlan::Join(_join_node) = &*mexpr.op().borrow() {
            let left = Rc::clone(&mexpr.operands()[0]);
            let right = Rc::clone(&mexpr.operands()[1]);
            vec![MExpr::build_with_node(mexpr.op(), vec![right, left])]
        } else {
            Vec::new()
        }
    }

    /// A clone of datafusion_optimizer::extract_equijoin_predicate
    /// This is not working exactly as expected since it cannot do equality inference across multiple joins
    /// For example : `Combined filter built : t1.a1 = t2.a2 AND t2.a2 = t3.a3, Left schema : fields:[t1.a1], metadata:{}, Right Schema fields:[t3.a3], metadata:{}, inferred equi-join clause []`
    /// `a1 = a3` should be inferred but isn't
    /// We will need to build this inference ourselves
    fn split_eq_and_noneq_join_predicate(
        &self,
        filter: Expr,
        left_schema: Arc<DFSchema>,
        right_schema: Arc<DFSchema>,
    ) -> Result<(Vec<(Expr, Expr)>, Option<Expr>)> {
        let exprs = split_conjunction_owned(filter);
        let inferred = infer_equalities(&exprs);
        //debug!("Inferred equalities : {:?}", inferred);

        let mut accum_join_keys: HashSet<(Expr, Expr)> = HashSet::new();
        let mut accum_filters: Vec<Expr> = vec![];
        for expr in exprs.into_iter().chain(inferred.into_iter()) {
            match expr {
                Expr::BinaryExpr(BinaryExpr {
                    ref left,
                    op: datafusion_expr::Operator::Eq,
                    ref right,
                }) => {
                    let join_key_pair = datafusion_expr::utils::find_valid_equijoin_key_pair(
                        left,
                        right,
                        &left_schema,
                        &right_schema,
                    )?;

                    if let Some((left_expr, right_expr)) = join_key_pair {
                        // TODO : Lot of un-necessary cloning here, fix me
                        if !accum_join_keys.contains(&(right_expr.clone(), left_expr.clone())) && !accum_join_keys.contains(&(left_expr.clone(), right_expr.clone())) {
                            accum_join_keys.insert((left_expr, right_expr));
                        }
                    } else {
                        accum_filters.push(expr);
                    }
                }
                _ => accum_filters.push(expr),
            }
        }

        let result_filter = accum_filters.into_iter().reduce(Expr::and);
        Ok((accum_join_keys.into_iter().collect(), result_filter))
    }

    // (A ⋈ B) ⋈ C  ==>  A ⋈ (B ⋈ C)
    fn apply_join_associativity(
        &self,
        mexpr: &MExpr,
        memo: &mut AHashMap<u64, Rc<RefCell<Group>>>,
    ) -> Vec<MExpr> {
        if let LogicalPlan::Join(_) = &*mexpr.op().borrow() {
            let mut result = Vec::new();

            let left = &mexpr.operands()[0];
            let right = &mexpr.operands()[1];

            let left_borrowed = left.borrow();
            let left_equivalent = left_borrowed.equivalent_logical_mexprs.borrow();

            // Check if left node is also a join
            let left_inner_joins: Vec<MExpr> = left_equivalent
                .iter()
                .filter(|x| matches!(*x.op().borrow(), LogicalPlan::Join(_)))
                .cloned()
                .collect();

            if left_inner_joins.is_empty() {
                return result; // No transformations possible
            }

            for left_mexpr in left_inner_joins {
                // Extract overall filter from left_mexpr and mexpr into a single conjunction
                // new up an empty vector of expressions
                let mut join_clause_plus_filters: Vec<Expr> = Vec::new();

                let left_mexpr_holder = left_mexpr.op();
                let left_op = left_mexpr_holder.borrow();
                let left_join = match &*left_op {
                    LogicalPlan::Join(join) => {
                        // Build a BinaryExpr from join.on
                        for (left, right) in &join.on {
                            let binary_expr = BinaryExpr::new(
                                Box::new(left.clone()),
                                Operator::Eq,
                                Box::new(right.clone()),
                            );
                            join_clause_plus_filters.push(Expr::BinaryExpr(binary_expr));
                        }

                        // Add join.filter if it exists
                        if let Some(filter) = &join.filter {
                            join_clause_plus_filters.push(filter.clone());
                        }

                        join
                    }
                    _ => continue,
                };

                let mexpr_op_holder = mexpr.op();
                let mexpr_op = mexpr_op_holder.borrow();
                let current_join = match &*mexpr_op {
                    LogicalPlan::Join(join) => {
                        // Build a BinaryExpr from join.on
                        for (left, right) in &join.on {
                            let binary_expr = BinaryExpr::new(
                                Box::new(left.clone()),
                                Operator::Eq,
                                Box::new(right.clone()),
                            );
                            join_clause_plus_filters.push(Expr::BinaryExpr(binary_expr));
                        }

                        // Add join.filter if it exists
                        if let Some(filter) = &join.filter {
                            join_clause_plus_filters.push(filter.clone());
                        }

                        join
                    }
                    _ => continue,
                };

                let combined_filter = conjunction(join_clause_plus_filters).unwrap_or(lit(true));

                let left_l = Rc::clone(&left_mexpr.operands()[0]);
                let left_r = Rc::clone(&left_mexpr.operands()[1]);

                let left_r_schema = match &left_r.borrow().start_expression {
                    Some(expr) => match expr.get_schema() {
                        Some(schema) => schema,
                        None => continue,
                    },
                    None => continue,
                };

                let right_schema = match &right.borrow().start_expression {
                    Some(expr) => match expr.get_schema() {
                        Some(schema) => schema,
                        None => continue,
                    },
                    None => continue,
                };

                // Derive the equi join clause and filter between for the new join node
                let (equi_join_clause, _other) = self
                    .split_eq_and_noneq_join_predicate(
                        combined_filter.clone(), //see if we can change to a Rc<Expr>
                        left_r_schema.clone(),
                        right_schema.clone(),
                    )
                    .unwrap();

                debug!(
                    "Combined filter built : {}, Left schema : {}, Right Schema {}, inferred equi-join clause {}",
                    combined_filter.to_string(),
                    left_r_schema.to_string(),
                    right_schema.to_string(),
                    format!("{:?}", equi_join_clause)
                );

                let left_r_schema_cloned = left_r_schema.clone();
                let right_schema_cloned = right_schema.clone();

                // Finally, build the new right join node
                let new_right_join_schema = Arc::new(
                    datafusion_expr::logical_plan::builder::build_join_schema(
                        &left_r_schema_cloned,
                        &right_schema_cloned,
                        &datafusion_expr::JoinType::Inner,
                    )
                    .unwrap(),
                );

                let new_right_join_node = LogicalPlan::Join(Join {
                    left: Arc::new(LogicalPlan::default()),
                    right: Arc::new(LogicalPlan::default()),
                    on: equi_join_clause,
                    filter: None, // HACK for now, we need to figure out residual filters
                    join_type: datafusion_expr::JoinType::Inner,
                    join_constraint: current_join.join_constraint,
                    schema: new_right_join_schema.clone(),
                    null_equality: current_join.null_equality,
                });

                debug!("New right join built : {}", new_right_join_node.display());

                // Build or fetch the group for this join node
                let new_right = self.gen_or_get_from_memo(
                    MExpr::build_with_node(
                        Rc::new(RefCell::new(new_right_join_node)),
                        vec![left_r, Rc::clone(right)],
                    ),
                    memo,
                );

                // Now build the final top-level join node
                let left_l_schema = match &left_l.borrow().start_expression {
                    Some(expr) => match expr.get_schema() {
                        Some(schema) => schema,
                        None => continue,
                    },
                    None => continue,
                };

                let (equi_join_clause2, _other2) = self
                    .split_eq_and_noneq_join_predicate(
                        combined_filter.clone(),
                        left_l_schema.clone(),
                        new_right_join_schema.clone(),
                    )
                    .unwrap();

                let left_l_schema_cloned = left_l_schema.clone();
                let new_right_schema_cloned = new_right_join_schema.clone();

                let new_top_join_node = LogicalPlan::Join(Join {
                    left: Arc::new(LogicalPlan::default()),
                    right: Arc::new(LogicalPlan::default()),
                    on: equi_join_clause2,
                    filter: None, // HACK for now
                    join_type: datafusion_expr::JoinType::Inner, // Preserve the original join type
                    join_constraint: left_join.join_constraint,
                    schema: Arc::new(
                        datafusion_expr::logical_plan::builder::build_join_schema(
                            &left_l_schema_cloned,
                            &new_right_schema_cloned,
                            &datafusion_expr::JoinType::Inner,
                        )
                        .unwrap(),
                    ),
                    null_equality: left_join.null_equality,
                });

                debug!("New top join built : {}", new_top_join_node.display());

                result.push(MExpr::build_with_node(
                    Rc::new(RefCell::new(new_top_join_node)),
                    vec![left_l, new_right],
                ));
            }

            result
        } else {
            Vec::new()
        }
    }

    /// For each transformed MExpr :
    /// 1. Check if it is already in the memo, if not add it to the memo with an association to the current group
    /// 2. And add it to the unexplored list
    fn add_new_mexprs(
        &mut self,
        group: &Rc<RefCell<Group>>,
        transformed: Vec<MExpr>,
        _rule_name: &str,
        memo: &mut AHashMap<u64, Rc<RefCell<Group>>>,
    ) {
        for new_expr in transformed {
            let hash = new_expr.hash();
            if !memo.contains_key(&hash) {
                // This is a newly generated transformation since it's missing from the memo
                memo.insert(hash, Rc::clone(group));
                group
                    .borrow_mut()
                    .unexplored_equivalent_logical_mexprs
                    .borrow_mut()
                    .push_back(new_expr);
            }

            // The transformed expression has been seen before, and it either
            // 1. Is already explored - no action needed here
            // 2. Added in the unexplored queue - no action needed here either
            // This way we avoid getting stuck in a loop since an already generated transformation is not re-explored
        }
    }

    fn gen_or_get_from_memo(
        &self,
        plan_mexpr: MExpr,
        memo: &mut AHashMap<u64, Rc<RefCell<Group>>>,
    ) -> Rc<RefCell<Group>> {
        let hash = plan_mexpr.hash();

        if let Some(group) = memo.get(&hash) {
            return Rc::clone(group);
        }

        // This subplan we have is either
        // 1. A brand-new plan with no equivalent logical plan that we've seen so far
        // or 2. We have generated a sub-plan of an existing Group but that group has not been explored so far

        let new_group = Group::from_mexpr(plan_mexpr);
        memo.insert(hash, Rc::clone(&new_group));
        new_group
    }

    pub fn test_match(&self, _match_against: &MExpr) -> bool {
        true
    }
}
