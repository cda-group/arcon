use crate::prelude::alloc::fmt::Formatter;
use metrics::register_gauge;
use perf_event::{events::Hardware, Counter, Group};
use serde::Deserialize;
use std::fmt;

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
}

impl fmt::Display for HardwareCounter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            HardwareCounter::CpuCycles => write!(f, "cpu_cycles"),
            HardwareCounter::Instructions => write!(f, "instructions"),
            HardwareCounter::CacheReferences => write!(f, "cache_references"),
            HardwareCounter::CacheMisses => write!(f, "cache_misses"),
            HardwareCounter::BranchInstructions => write!(f, "branch_instructions"),
            HardwareCounter::BranchMisses => write!(f, "branch_misses"),
            HardwareCounter::BusCycles => write!(f, "bus_cycles"),
            HardwareCounter::StalledCyclesFrontend => write!(f, "stalled_cycles_frontend"),
            HardwareCounter::StalledCyclesBackend => write!(f, "stalled_cycles_backend"),
            HardwareCounter::RefCpuCycles => write!(f, "ref_cpu_cycles"),
        }
    }
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct PerfEvents {
    pub counters: Vec<HardwareCounter>,
}

impl PerfEvents {
    pub fn new() -> PerfEvents {
        PerfEvents { counters: vec![] }
    }

    pub fn add(&mut self, hardware_metric_kind: HardwareCounter) {
        self.counters.push(hardware_metric_kind);
    }
}

pub struct HardwareMetricGroup {
    pub group: Group,
    pub counters: Vec<(String, Counter)>,
}

impl HardwareMetricGroup {
    pub(crate) fn register_performance_metric_gauges(
        &mut self,
        node_name: String,
        perf_events: PerfEvents,
    ) -> std::io::Result<()> {
        let iterator = perf_events.counters.iter();
        for value in iterator {
            register_gauge!(self.get_field_gauge_name(&node_name, &value.to_string()));
        }
        self.group.enable()
    }

    pub fn get_field_gauge_name(&self, field_name: &str, node_name: &str) -> String {
        format!("{}_{}", node_name, field_name)
    }
}
