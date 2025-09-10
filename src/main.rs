use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_expr::logical_plan::LogicalPlanBuilder;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_common::tree_node::TreeNode;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use crate::cascades::cascades::Cascades;
mod planprinter;
mod join_graph;

pub mod cascades;

use planprinter::PlanStringBuilder;

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


// Custom print method that uses the TreeNodeVisitor implementation
fn custom_print(plan: &LogicalPlan) -> Result<String, Box<dyn std::error::Error>> {
    let mut builder = PlanStringBuilder::new();
    plan.visit(&mut builder)?;
    Ok(builder.get_output())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating DataFusion logical plan with 4 tables and joins...");

    let ctx = setup_tables().await?;

    // Step 2: Create a logical plan that emulates:
    // SELECT 1 FROM t1 inner join t2 on a1=a2 inner join t3 on a2=a3 inner join t4 on a1=a4
    // Using DataFusion DataFrame API
    
    let t1 = ctx.table("t1").await?;
    let t2 = ctx.table("t2").await?;
    let t3 = ctx.table("t3").await?;
    let t4 = ctx.table("t4").await?;
    
    // Build a join graph
    let logical_plan = LogicalPlanBuilder::from((*t1.logical_plan()).clone())
        .join((*t2.logical_plan()).clone(), JoinType::Inner, (vec!["a1"], vec!["a2"]), None)?
        .join((*t3.logical_plan()).clone(), JoinType::Inner, (vec!["a2"], vec!["a3"]), None)?
        .join((*t4.logical_plan()).clone(), JoinType::Inner, (vec!["a3"], vec!["a4"]), None)?
        .project(vec![lit(1)])? // SELECT 1
        .build()?;

    // Print the logical plan using display_indent()
    // println!("Logical Plan for complex joins:");
    // println!("{}", logical_plan.display_indent());
    
    // Print using custom_print method
    println!("Formatted plan:");
    let custom_output = custom_print(&logical_plan)?;
    println!("{}", custom_output);
    
    // Extract and display join graph
    // println!("\nJoin Graph:");
    // let join_graph = JoinGraph::from_plan(&logical_plan)?;
    // println!("Join expressions: {:?}", join_graph.join_expressions);
    // println!("Sources count: {}", join_graph.sources.len());
    // for (i, source) in join_graph.sources.iter().enumerate() {
    //     println!("Source {}: {:?}", i, std::mem::discriminant(source));
    // }
    
    // println!("{}", logical_plan.display_pg_json());

    //New up a Cascades optimizer and optimize the plan
    let mut cascades = Cascades::new();
    let root_group = cascades.gen_group_logical_plan(Rc::new(RefCell::new(logical_plan)));

    println!("Memo before Cascades optimizer start");
    cascades.print_memo();
    
    cascades.optimize(root_group);

    //Print memo stats
    cascades.print_memo_stats();
    

    Ok(())
}
