use crate::metrics::histogram::Histogram;

// TODO
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    pub throughput: u64
}

impl NodeMetrics {
    pub fn new() -> NodeMetrics {
        NodeMetrics {
            throughput: 0,
        }
    }
}
