use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;
use crate::cascades::Cascades;
use crate::cascades::util::get_cheapest_tree;
mod planprinter;
mod join_graph;

pub mod cascades;

use crate::cascades::test_utils;

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

    // Step 2: Dynamically create a logical plan for a left-deep join tree
    let logical_plan = test_utils::generate_logical_plan(table_row_counts).await;

    // Print the logical plan using display_indent()
    // println!("Logical Plan for complex joins:");
    // println!("{}", logical_plan.display_indent());

    // Print using custom_print method
    println!("Formatted plan:");
    let custom_output = test_utils::custom_print(&logical_plan)?;
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
