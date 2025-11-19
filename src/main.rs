use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_expr::logical_plan::LogicalPlanBuilder;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_common::tree_node::TreeNode;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use crate::cascades::Cascades;
use crate::cascades::util::get_cheapest_tree;
mod planprinter;
mod join_graph;

pub mod cascades;

use planprinter::PlanStringBuilder;

async fn setup_tables(table_count: usize) -> Result<SessionContext, Box<dyn std::error::Error>> {
    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Step 1: Dynamically create tables with integer columns (a1, a2, ..., an)
    for i in 1..=table_count {
        let column_name = format!("a{}", i);
        let schema = Arc::new(Schema::new(vec![
            Field::new(&column_name, DataType::Int32, false),
        ]));
        let data = Int32Array::from((1..=5).map(|x| (x * i) as i32).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)])?;
        ctx.register_batch(&format!("t{}", i), batch)?;
    }

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
    // Initialize the logger
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let table_row_counts: Vec<usize> = args.get(1)
        .map(|arg| arg.split(',')
            .filter_map(|s| s.trim().parse::<usize>().ok())
            .collect())
        .unwrap_or_else(|| vec![4]); // Default to a single table with 4 rows if no valid argument is provided

    let table_count = table_row_counts.len();

    println!("Creating DataFusion logical plan with {} tables and joins...", table_count);

    let ctx = setup_tables(table_count).await?;

    // Step 2: Dynamically create a logical plan for a left-deep join tree
    let mut logical_plan = None;

    for i in 1..=table_count {
        let table_name = format!("t{}", i);
        let table = ctx.table(&table_name).await?;

        let mut table_scan = match table.logical_plan() {
            LogicalPlan::TableScan(scan) => scan.clone(),
            _ => panic!("Expected a TableScan node"),
        };

        table_scan.fetch = Some(table_row_counts[i - 1]);

        if let Some(plan) = logical_plan {
            let left_column = format!("a{}", i - 1);
            let right_column = format!("a{}", i);
            logical_plan = Some(
                LogicalPlanBuilder::from(plan)
                    .join(LogicalPlan::TableScan(table_scan), JoinType::Inner, (vec![left_column], vec![right_column]), None)?
                    .build()?,
            );
        } else {
            logical_plan = Some(LogicalPlan::TableScan(table_scan));
        }
    }

    // Add a projection to select a constant value (e.g., SELECT 1)
    let logical_plan = LogicalPlanBuilder::from(logical_plan.unwrap())
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
    let mut cascades = Cascades::default();
    let root_group = cascades.gen_group_logical_plan(Rc::new(RefCell::new(logical_plan)));

    println!("Memo before starting optimization:");
    cascades.print_memo();

    let start_time = Instant::now();
    cascades.optimize(root_group.clone());
    let duration = start_time.elapsed();
    println!("Optimization completed in: {:.2?}", duration);
    
    //Print memo stats
    println!("Memo stats");
    cascades.print_memo_stats();

    println!("Cheapest plan:");
    println!("{}",  get_cheapest_tree(root_group.clone()));

    // println!("Generating all possible join trees");
    // let all_trees = get_all_possible_trees(root_group);
    
    // println!("Writing these to output.txt");
    // let mut file = std::fs::File::create("output.txt").unwrap();
    // use std::io::Write;
    // for tree in all_trees {
    //     writeln!(file, "{}", tree).unwrap();
    // }
    // file.flush().unwrap();
    

    Ok(())
}
