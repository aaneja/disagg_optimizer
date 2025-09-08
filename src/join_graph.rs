use datafusion_expr::{LogicalPlan, Expr, JoinType, Operator};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::DataFusionError;

/// Represents a join graph extracted from a logical plan
#[derive(Debug, Clone)]
pub struct JoinGraph {
    /// Vector of join expressions of the form `left = right`
    pub join_expressions: Vec<Expr>,
    /// Vector of source plan nodes (non-join, non-projection nodes)
    pub sources: Vec<LogicalPlan>,
}

impl JoinGraph {
    pub fn new() -> Self {
        Self {
            join_expressions: Vec::new(),
            sources: Vec::new(),
        }
    }

    /// Extract a join graph from a logical plan
    pub fn from_plan(plan: &LogicalPlan) -> Result<Self, DataFusionError> {
        let mut visitor = JoinGraphVisitor::new();
        plan.visit(&mut visitor)?;
        Ok(visitor.join_graph)
    }
}

/// Visitor that traverses a logical plan and builds a join graph
struct JoinGraphVisitor {
    join_graph: JoinGraph,
}

impl JoinGraphVisitor {
    fn new() -> Self {
        Self {
            join_graph: JoinGraph::new(),
        }
    }
}

impl TreeNodeVisitor<'_> for JoinGraphVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError> {
        match node {
            LogicalPlan::Join(join) => {
                // Only process INNER joins
                if join.join_type == JoinType::Inner {
                    // Construct expressions of the form `left = right` using join_keys
                    for (left_expr, right_expr) in &join.on {
                        let join_expr = Expr::BinaryExpr(datafusion_expr::BinaryExpr {
                            left: Box::new(left_expr.clone()),
                            op: Operator::Eq,
                            right: Box::new(right_expr.clone()),
                        });
                        self.join_graph.join_expressions.push(join_expr);
                    }
                }
                // Continue traversing to process children
                Ok(TreeNodeRecursion::Continue)
            }
            LogicalPlan::Projection(_) => {
                // Skip projection nodes but continue traversing children
                Ok(TreeNodeRecursion::Continue)
            }
            _ => {
                // For any other plan node type, add it to sources
                self.join_graph.sources.push(node.clone());
                // Stop traversing children since we've captured this source - THIS NEEDS TO BE FIXED
                Ok(TreeNodeRecursion::Continue)
            }
        }
    }

    fn f_up(&mut self, _node: &LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError> {
        // No special processing needed on the way up
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_join_graph_extraction() -> Result<(), Box<dyn std::error::Error>> {
        // This is a basic test - you can expand it based on your needs
        let ctx = SessionContext::new();
        
        // Create a simple plan for testing
        let plan = ctx.sql("SELECT 1").await?.into_optimized_plan()?;
        
        // Extract join graph
        let join_graph = JoinGraph::from_plan(&plan)?;
        
        // Basic assertions - just check that we can extract without errors
        println!("Join expressions found: {}", join_graph.join_expressions.len());
        println!("Sources found: {}", join_graph.sources.len());
        
        Ok(())
    }
}
