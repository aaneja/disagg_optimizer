use crate::cascades::cascades::Cascades;
use datafusion_common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion_expr::LogicalPlan;

// Custom string builder for formatting the logical plan
pub struct PlanStringBuilder {
    output: String,
    depth: usize,
}

impl PlanStringBuilder {
    pub fn new() -> Self {
        Self {
            output: String::new(),
            depth: 0,
        }
    }

    fn add_line(&mut self, text: &str) {
        let indent = "  ".repeat(self.depth);
        self.output.push_str(&format!("{}{}\n", indent, text));
    }

    fn increase_depth(&mut self) {
        self.depth += 1;
    }

    fn decrease_depth(&mut self) {
        if self.depth > 0 {
            self.depth -= 1;
        }
    }

    pub fn get_output(self) -> String {
        self.output
    }
}

impl TreeNodeVisitor<'_> for PlanStringBuilder {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &LogicalPlan) -> Result<TreeNodeRecursion, datafusion_common::DataFusionError> {
        // Add your custom formatting logic here
        match node {
            LogicalPlan::Projection(proj) => {
                self.add_line(&format!("PROJECTION: {:?}", proj.expr));
            },
            LogicalPlan::Join(join) => {
                self.add_line(&format!("JOIN: {:?} ON {:?}", join.join_type, join.on));
            },
            LogicalPlan::TableScan(scan) => {
                self.add_line(&format!("TABLE_SCAN: {}", scan.table_name));
            },
            _ => {
                self.add_line(&format!("NODE: {:?}", std::mem::discriminant(node)));
            }
        }
        
        self.increase_depth();
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _node: &LogicalPlan) -> Result<TreeNodeRecursion, datafusion_common::DataFusionError> {
        self.decrease_depth();
        Ok(TreeNodeRecursion::Continue)
    }
}
