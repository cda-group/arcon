use metrics::{
    counter, decrement_gauge, gauge, histogram, increment_counter, increment_gauge,
    register_counter, register_gauge, register_histogram, GaugeValue, Key, Recorder, Unit,
};
use perf_event::{events::Hardware, Counter, Group};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub enum HardwareCounter {
    CPU_CYCLES,
    BRANCH_MISSES,
    INSTRUCTIONS,
    CACHE_REFERENCES,
    CACHE_MISSES,
    BRANCH_INSTRUCTIONS,
    BUS_CYCLES,
    STALLED_CYCLES_FRONTEND,
    STALLED_CYCLES_BACKEND,
    REF_CPU_CYCLES,
}

impl HardwareCounter {
    pub(crate) fn get_hardware_kind(&self) -> Hardware {
        match self {
            HardwareCounter::CPU_CYCLES => Hardware::CPU_CYCLES,
            HardwareCounter::INSTRUCTIONS => Hardware::INSTRUCTIONS,
            HardwareCounter::CACHE_REFERENCES => Hardware::CACHE_REFERENCES,
            HardwareCounter::CACHE_MISSES => Hardware::CACHE_MISSES,
            HardwareCounter::BRANCH_INSTRUCTIONS => Hardware::BRANCH_INSTRUCTIONS,
            HardwareCounter::BRANCH_MISSES => Hardware::BRANCH_MISSES,
            HardwareCounter::BUS_CYCLES => Hardware::BUS_CYCLES,
            HardwareCounter::STALLED_CYCLES_FRONTEND => Hardware::STALLED_CYCLES_FRONTEND,
            HardwareCounter::STALLED_CYCLES_BACKEND => Hardware::STALLED_CYCLES_BACKEND,
            HardwareCounter::REF_CPU_CYCLES => Hardware::REF_CPU_CYCLES,
        }
    }

    pub(crate) fn get_counter_as_string(&self) -> &str {
        match self {
            HardwareCounter::CPU_CYCLES => "CPU_CYCLES",
            HardwareCounter::INSTRUCTIONS => "INSTRUCTIONS",
            HardwareCounter::CACHE_REFERENCES => "CACHE_REFERENCES",
            HardwareCounter::CACHE_MISSES => "CACHE_MISSES",
            HardwareCounter::BRANCH_INSTRUCTIONS => "BRANCH_INSTRUCTIONS",
            HardwareCounter::BRANCH_MISSES => "BRANCH_MISSES",
            HardwareCounter::BUS_CYCLES => "BUS_CYCLES",
            HardwareCounter::STALLED_CYCLES_FRONTEND => "STALLED_CYCLES_FRONTEND",
            HardwareCounter::STALLED_CYCLES_BACKEND => "STALLED_CYCLES_BACKEND",
            HardwareCounter::REF_CPU_CYCLES => "REF_CPU_CYCLES",
        }
    }
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct PerfEvents {
    pub hardware_metric_kind_vector: Vec<HardwareCounter>,
}

impl PerfEvents {
    pub fn new() -> PerfEvents {
        PerfEvents {
            hardware_metric_kind_vector: vec![],
        }
    }

    pub fn add(&mut self, hardware_metric_kind: HardwareCounter) {
        self.hardware_metric_kind_vector.push(hardware_metric_kind);
    }
}

pub struct PerformanceMetric {
    pub performance_metrics_group: Group,
    pub hardware_metric_counters: Vec<(String, Counter)>,
}

impl PerformanceMetric {
    pub(crate) fn register_performance_metric_gauges(
        &mut self,
        node_name: String,
        perf_events: PerfEvents,
    ) -> std::io::Result<()> {
        let iterator = perf_events.hardware_metric_kind_vector.iter();
        for value in iterator {
            register_gauge!(self.get_field_gauge_name(&node_name, value.get_counter_as_string()));
        }
        self.performance_metrics_group.enable()
    }

    pub fn get_field_gauge_name(&self, field_name: &str, node_name: &str) -> String {
        [node_name, field_name].join("\n")
    }
}
