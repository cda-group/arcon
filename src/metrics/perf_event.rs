use metrics::register_gauge;
use perf_event::{events::Hardware, Counter, Group};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub enum HardwareCounter {
    CpuCycles,
    BranchMisses,
    Instructions,
    CacheReferences,
    CacheMisses,
    BranchInstructions,
    BusCycles,
    StalledCyclesFrontend,
    StalledCyclesBackend,
    RefCpuCycles,
}

impl HardwareCounter {
    pub(crate) fn get_hardware_kind(&self) -> Hardware {
        match self {
            HardwareCounter::CpuCycles => Hardware::CPU_CYCLES,
            HardwareCounter::Instructions => Hardware::INSTRUCTIONS,
            HardwareCounter::CacheReferences => Hardware::CACHE_REFERENCES,
            HardwareCounter::CacheMisses => Hardware::CACHE_MISSES,
            HardwareCounter::BranchInstructions => Hardware::BRANCH_INSTRUCTIONS,
            HardwareCounter::BranchMisses => Hardware::BRANCH_MISSES,
            HardwareCounter::BusCycles => Hardware::BUS_CYCLES,
            HardwareCounter::StalledCyclesFrontend => Hardware::STALLED_CYCLES_FRONTEND,
            HardwareCounter::StalledCyclesBackend => Hardware::STALLED_CYCLES_BACKEND,
            HardwareCounter::RefCpuCycles => Hardware::REF_CPU_CYCLES,
        }
    }

    pub(crate) fn get_counter_as_string(&self) -> &str {
        match self {
            HardwareCounter::CpuCycles => "CpuCycles",
            HardwareCounter::Instructions => "Instructions",
            HardwareCounter::CacheReferences => "CacheReferences",
            HardwareCounter::CacheMisses => "CacheMisses",
            HardwareCounter::BranchInstructions => "BranchInstructions",
            HardwareCounter::BranchMisses => "BranchMisses",
            HardwareCounter::BusCycles => "BusCycles",
            HardwareCounter::StalledCyclesFrontend => "StalledCyclesFrontend",
            HardwareCounter::StalledCyclesBackend => "StalledCyclesBackend",
            HardwareCounter::RefCpuCycles => "RefCpuCycles",
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
        format!("{}_{}", node_name, field_name)
    }
}
