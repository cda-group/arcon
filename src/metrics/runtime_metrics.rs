use crate::metrics::meter::Meter;
use metrics::{register_counter, register_gauge, register_histogram};

#[derive(Clone, Debug, Default)]
pub struct NodeMetrics {
    pub inbound_throughput: Meter,
}

impl NodeMetrics {
    pub fn new(node_name: &str) -> NodeMetrics {
        register_gauge!(format!("{}_{}", node_name, "inbound_throughput"));
        register_counter!(format!("{}_{}", node_name, "epoch_counter"));
        register_counter!(format!("{}_{}", node_name, "watermark_counter"));
        register_histogram!(format!("{}_{}", node_name, "batch_execution_time"));

        NodeMetrics {
            inbound_throughput: Meter::new(),
        }
    }
}
pub struct SourceMetrics {
    pub incoming_message_rate: Meter,
}
impl SourceMetrics {
    pub fn new(source_node_name: &str) -> SourceMetrics {
        register_gauge!(format!("{}_{}", source_node_name, "incoming_message_rate"));
        register_counter!(format!("{}_{}", source_node_name, "error_counter"));
        SourceMetrics {
            incoming_message_rate: Meter::new(),
        }
    }
}
