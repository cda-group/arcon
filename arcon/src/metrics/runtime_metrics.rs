use crate::metrics::meter::Meter;

#[derive(Clone, Debug, Default)]
pub struct NodeMetrics {
    pub inbound_throughput: Meter,
}

impl NodeMetrics {
    pub fn new() -> NodeMetrics {
        NodeMetrics {
            inbound_throughput: Meter::new(),
        }
    }
}
pub struct SourceMetrics {
    pub incoming_message_rate: Meter,
}
impl SourceMetrics {
    pub fn new() -> SourceMetrics {
        SourceMetrics {
            incoming_message_rate: Meter::new(),
        }
    }
}
