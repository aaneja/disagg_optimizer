use optimizer::cascades::Cascades;
use optimizer::util::{get_all_possible_trees, get_all_possible_trees_count};
use std::time::Instant;

fn main() {
    for _ in 0..1 {
        driver();
    }
}

fn driver() {
    println!("Testing Rust Cascades Optimizer");
    
    let mut cascades = Cascades::new();

    // read join_node string from command line
    let join_nodes = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: {} <join_nodes>", std::env::args().nth(0).unwrap());
        std::process::exit(1);
    });

    let root_group = cascades.seed_memo(&join_nodes);
    println!(
        "Seeded Memo : {}",
        root_group
            .borrow()
            .start_expression
            .as_ref()
            .unwrap()
            .get_full_string()
    );
    
    println!("Created root group for join nodes '{join_nodes}'");
    cascades.print_memo();
    
    // Run optimization
    let start_time = Instant::now();
    cascades.optimize(root_group.clone());
    let duration = start_time.elapsed();
    println!("Optimization completed in: {:.2?}", duration);
    
    println!("After optimization:");
    cascades.print_memo_stats();
    
    // println!(
    //     "All possible tree count : {}",
    //     get_all_possible_trees_count(root_group)
    // );

    println!("Generating all possible join trees");
    let all_trees = get_all_possible_trees(root_group);
    println!("Writing these to output.txt");
    let mut file = std::fs::File::create("output.txt").unwrap();
    use std::io::Write;
    for tree in all_trees {
        writeln!(file, "{}", tree).unwrap();
    }
    file.flush().unwrap();
}
