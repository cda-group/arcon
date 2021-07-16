use crate::metrics::meter::Meter;
use metrics::{
    counter, decrement_gauge, gauge, histogram, increment_counter, increment_gauge,
    register_counter, register_gauge, register_histogram, GaugeValue, Key, Recorder, Unit,
};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug, Default)]
pub struct NodeRuntimeMetrics {
    pub inbound_throughput: InboundThroughput,
}

impl NodeRuntimeMetrics {
    pub fn new(node_name: &str) -> NodeRuntimeMetrics {
        NodeRuntimeMetrics {
            inbound_throughput: InboundThroughput::new(node_name),
        }
    }
}

pub trait MetricValue {
    fn get_value(&mut self) -> f64;
    fn update_value(&mut self, value: u64);
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct InboundThroughput {
    meter: Meter,
}

impl MetricValue for InboundThroughput {
    fn get_value(&mut self) -> f64 {
        self.meter.get_one_min_rate()
    }

    fn update_value(&mut self, value: u64) {
        self.meter.mark_n(value)
    }
}

impl InboundThroughput {
    pub fn new(node_name: &str) -> InboundThroughput {
        register_gauge!([node_name, "_inbound_throughput"].join("\n"));
        InboundThroughput {
            meter: Meter::new(),
        }
    }
}
