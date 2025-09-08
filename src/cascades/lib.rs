pub mod cascades;
pub mod group;
pub mod mexpr;
pub mod rulematcher;
pub mod sourcenode;
pub mod operator;
pub mod util;

#[cfg(test)]
mod tests {
    use crate::cascades::Cascades;
    use crate::group::Group;
    use crate::mexpr::MExpr;
    use crate::operator::Operator;

    #[test]
    fn test_basic_optimization() {
        let mut cascades = Cascades::new();

        // Test with a simple 2-node join: "12"
        let root_group = cascades.seed_memo("12");

        // Verify the root group was created
        assert!(!root_group.is_explored());

        // Run optimization
        cascades.optimize(root_group.clone());

        // Verify optimization completed
        assert!(root_group.is_explored());

        // Check that memo contains entries
        assert!(!cascades.get_memo().is_empty());

        println!("Test passed: Basic optimization works correctly");
    }

    #[test]
    fn test_source_node_creation() {
        let source_group = Group::get_source_node_group("A".to_string());

        assert!(source_group.source_node.is_some());
        assert_eq!(source_group.source_node.as_ref().unwrap().node_id, "A");

        println!("Test passed: Source node creation works correctly");
    }

    #[test]
    fn test_mexpr_hashing() {
        let source_a = Group::get_source_node_group("A".to_string());
        let source_b = Group::get_source_node_group("B".to_string());

        let mexpr1 = MExpr::build_with(Operator::InnerJoin, vec![source_a.clone(), source_b.clone()]);
        let mexpr2 = MExpr::build_with(Operator::InnerJoin, vec![source_a, source_b]);

        // Same inputs should produce same hash
        assert_eq!(mexpr1.hash(), mexpr2.hash());

        println!("Test passed: MExpr hashing works correctly");
    }
}
