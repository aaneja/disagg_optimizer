#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceNode {
    pub node_id: String,
}

impl SourceNode {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }
}
