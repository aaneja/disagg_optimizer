use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_expr::LogicalPlan;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use std::sync::Arc;

async fn setup_tables() -> Result<SessionContext, Box<dyn std::error::Error>> {
    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Step 1: Create 4 tables (t1, t2, t3, t4) with integer columns (a1, a2, a3, a4)
    
    // Table t1 with column a1
    let schema_t1 = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
    ]));
    let data_t1 = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let batch_t1 = RecordBatch::try_new(schema_t1.clone(), vec![Arc::new(data_t1)])?;
    ctx.register_batch("t1", batch_t1)?;

    // Table t2 with column a2
    let schema_t2 = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
    ]));
    let data_t2 = Int32Array::from(vec![1, 2, 3, 6, 7]);
    let batch_t2 = RecordBatch::try_new(schema_t2.clone(), vec![Arc::new(data_t2)])?;
    ctx.register_batch("t2", batch_t2)?;

    // Table t3 with column a3
    let schema_t3 = Arc::new(Schema::new(vec![
        Field::new("a3", DataType::Int32, false),
    ]));
    let data_t3 = Int32Array::from(vec![1, 2, 8, 9, 10]);
    let batch_t3 = RecordBatch::try_new(schema_t3.clone(), vec![Arc::new(data_t3)])?;
    ctx.register_batch("t3", batch_t3)?;

    // Table t4 with column a4
    let schema_t4 = Arc::new(Schema::new(vec![
        Field::new("a4", DataType::Int32, false),
    ]));
    let data_t4 = Int32Array::from(vec![1, 3, 5, 11, 12]);
    let batch_t4 = RecordBatch::try_new(schema_t4.clone(), vec![Arc::new(data_t4)])?;
    ctx.register_batch("t4", batch_t4)?;

    Ok(ctx)
}

// Custom string builder for formatting the logical plan
struct PlanStringBuilder {
    output: String,
    depth: usize,
}

impl PlanStringBuilder {
    fn new() -> Self {
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

    fn get_output(self) -> String {
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

// Custom print method that uses the TreeNodeVisitor implementation
fn custom_print(plan: &LogicalPlan) -> Result<String, Box<dyn std::error::Error>> {
    let mut builder = PlanStringBuilder::new();
    plan.visit(&mut builder)?;
    Ok(builder.get_output())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating DataFusion logical plan with 4 tables and joins...");

    let ctx = setup_tables().await?;

    // Step 2: Create a logical plan that emulates:
    // SELECT 1 FROM t1 inner join t2 on a1=a2 left join t3 on a2=a3 left join t4 on a1=a4
    // Using DataFusion DataFrame API
    
    let t1 = ctx.table("t1").await?;
    let t2 = ctx.table("t2").await?;
    let t3 = ctx.table("t3").await?;
    let t4 = ctx.table("t4").await?;
    
    // Build the joins step by step
    let joined_df = t1
        .join(t2, JoinType::Inner, &["a1"], &["a2"], None)?
        .join(t3, JoinType::Left, &["a2"], &["a3"], None)?
        .join(t4, JoinType::Left, &["a1"], &["a4"], None)?
        .select(vec![lit(1)])?; // SELECT 1
    
    let logical_plan = joined_df.into_optimized_plan()?;

    // Print the logical plan using display_indent()
    // println!("Logical Plan for complex joins:");
    // println!("{}", logical_plan.display_indent());
    
    // Print using custom_print method
    println!("\nCustom formatted plan:");
    let custom_output = custom_print(&logical_plan)?;
    println!("{}", custom_output);
    
    // println!("{}", logical_plan.display_pg_json());

    Ok(())
}
