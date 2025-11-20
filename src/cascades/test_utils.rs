use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, lit};

use crate::planprinter::PlanStringBuilder;
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_common::JoinType;
use datafusion_common::tree_node::TreeNode;
use std::sync::Arc;

pub async fn generate_logical_plan(table_row_counts: Vec<usize>) -> LogicalPlan {
    let table_count: usize = table_row_counts.len();
    let ctx = setup_tables(table_count).ok().unwrap();

    // Step 2: Dynamically create a logical plan for a left-deep join tree
    let mut logical_plan = None;

    for i in 1..=table_count {
        let table_name = format!("t{}", i);
        let table = ctx.table(&table_name).await.ok().unwrap();

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
                    .join(
                        LogicalPlan::TableScan(table_scan),
                        JoinType::Inner,
                        (vec![left_column], vec![right_column]),
                        None,
                    )
                    .ok()
                    .unwrap()
                    .build()
                    .ok()
                    .unwrap(),
            );
        } else {
            logical_plan = Some(LogicalPlan::TableScan(table_scan));
        }
    }

    // Add a projection to select a constant value (e.g., SELECT 1)
    let logical_plan = LogicalPlanBuilder::from(logical_plan.unwrap())
        .project(vec![lit(1)])
        .ok()
        .unwrap() // SELECT 1
        .build()
        .ok()
        .unwrap();
    logical_plan
}

pub fn setup_tables(table_count: usize) -> Result<SessionContext, Box<dyn std::error::Error>> {
    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Step 1: Dynamically create tables with integer columns (a1, a2, ..., an)
    for i in 1..=table_count {
        let column_name = format!("a{}", i);
        let schema = Arc::new(Schema::new(vec![Field::new(
            &column_name,
            DataType::Int32,
            false,
        )]));
        let data = Int32Array::from((1..=5).map(|x| (x * i) as i32).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)])
            .ok()
            .unwrap();
        ctx.register_batch(&format!("t{}", i), batch).ok().unwrap();
    }

    Ok(ctx)
}

pub fn custom_print(plan: &LogicalPlan) -> Result<String, Box<dyn std::error::Error>> {
    let mut builder = PlanStringBuilder::new();
    plan.visit(&mut builder)?;
    Ok(builder.get_output())
}
