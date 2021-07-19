use crate::metrics::meter::Meter;
use metrics::{
    counter, decrement_gauge, gauge, histogram, increment_counter, increment_gauge,
    register_counter, register_gauge, register_histogram, GaugeValue, Key, Recorder, Unit,
};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug, Default)]
pub struct NodeRuntimeMetrics {
    pub inbound_throughput: InboundThroughput,
    pub epoch_counter: EpochCounter,
    pub watermark_counter: WatermarkCounter,
}

impl NodeRuntimeMetrics {
    pub fn new(node_name: &str) -> NodeRuntimeMetrics {
        NodeRuntimeMetrics {
            inbound_throughput: InboundThroughput::new(node_name),
            epoch_counter: EpochCounter::new(node_name),
            watermark_counter: WatermarkCounter::new(node_name),
        }
    }
}
pub struct SourceNodeRuntimeMetrics {
    pub incoming_message_rate: IncomingMessageRate,
    pub error_counter: ErrorCounter,
}
impl SourceNodeRuntimeMetrics {
    pub fn new(source_node_name: &str) -> SourceNodeRuntimeMetrics {
        SourceNodeRuntimeMetrics {
            incoming_message_rate: IncomingMessageRate::new(source_node_name),
            error_counter: ErrorCounter::new(source_node_name),
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

#[derive(Deserialize, Clone, Debug, Default)]
pub struct EpochCounter {
    counter_value: u64,
}

impl EpochCounter {
    pub fn new(node_name: &str) -> EpochCounter {
        register_gauge!([node_name, "_epoch_counter"].join("\n"));
        EpochCounter { counter_value: 0 }
    }
}

impl MetricValue for EpochCounter {
    fn get_value(&mut self) -> f64 {
        self.counter_value as f64
    }

    fn update_value(&mut self, value: u64) {
        self.counter_value += value
    }
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct WatermarkCounter {
    counter_value: u64,
}

impl WatermarkCounter {
    pub fn new(node_name: &str) -> WatermarkCounter {
        register_gauge!([node_name, "_watermark_counter"].join("\n"));
        WatermarkCounter { counter_value: 0 }
    }
}

impl MetricValue for WatermarkCounter {
    fn get_value(&mut self) -> f64 {
        self.counter_value as f64
    }

    fn update_value(&mut self, value: u64) {
        self.counter_value += value
    }
}

pub struct IncomingMessageRate {
    meter: Meter,
}

impl IncomingMessageRate {
    pub fn new(node_name: &str) -> IncomingMessageRate {
        register_gauge!([node_name, "_incoming_message_rate"].join("\n"));
        IncomingMessageRate {
            meter: Meter::new(),
        }
    }
}

impl MetricValue for IncomingMessageRate {
    fn get_value(&mut self) -> f64 {
        self.meter.get_one_min_rate()
    }

    fn update_value(&mut self, value: u64) {
        self.meter.mark_n(value);
    }
}

pub struct ErrorCounter {
    counter_value: u64,
}

impl ErrorCounter {
    pub fn new(source_node_name: &str) -> ErrorCounter {
        register_gauge!([source_node_name, "_error_counter"].join("\n"));
        ErrorCounter { counter_value: 0 }
    }
}

impl MetricValue for ErrorCounter {
    fn get_value(&mut self) -> f64 {
        self.counter_value as f64
    }

    fn update_value(&mut self, value: u64) {
        self.counter_value += value;
    }
}
