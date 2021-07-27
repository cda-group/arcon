use crate::metrics::meter::Meter;
use metrics::{register_counter, register_gauge, register_histogram};

#[derive(Clone, Debug, Default)]
pub struct NodeMetrics {
    pub inbound_throughput: Meter,
    pub epoch_counter: u64,
    pub watermark_counter: u64,
    pub batch_execution_time: f64,
}

impl NodeMetrics {
    pub fn new(node_name: &str) -> NodeMetrics {
        register_gauge!(format!("{}_{}", node_name, "inbound_throughput"));
        register_counter!(format!("{}_{}", node_name, "epoch_counter"));
        register_counter!(format!("{}_{}", node_name, "watermark_counter"));
        register_histogram!(format!("{}_{}", node_name, "batch_execution_time"));

        NodeMetrics {
            inbound_throughput: Meter::new(),
            epoch_counter: 0,
            watermark_counter: 0,
            batch_execution_time: 0.0,
        }
    }

    pub fn increment_epoch_counter(&mut self) {
        self.epoch_counter += 1;
    }

    pub fn increment_watermark_counter(&mut self) {
        self.watermark_counter += 1;
    }
}
pub struct SourceMetrics {
    pub incoming_message_rate: Meter,
    pub error_counter: u64,
}
impl SourceMetrics {
    pub fn new(source_node_name: &str) -> SourceMetrics {
        register_gauge!(format!("{}_{}", source_node_name, "incoming_message_rate"));
        register_counter!(format!("{}_{}", source_node_name, "error_counter"));
        SourceMetrics {
            incoming_message_rate: Meter::new(),
            error_counter: 0,
        }
    }
    pub fn increment_error_counter(&mut self) {
        self.error_counter += 1;
    }
}
