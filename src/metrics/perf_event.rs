use crate::prelude::alloc::fmt::Formatter;
use perf_event::events::Hardware;
use serde::Deserialize;
use std::fmt;

/// An enum representing supported hardware counters with perf events as enum options
///
/// It is a wrapper around [Hardware] in order to support [Deserialize]
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

/// Configurable hardware counters that may be added to an Operator's config
///
///```no_run
/// use arcon::prelude::*;
/// fn main() {
///     let mut perf_events = PerfEvents::new();
///     perf_events.add(HardwareCounter::CpuCycles);
///     perf_events.add(HardwareCounter::BranchMisses);
///     let mut app = Application::default()
///         .iterator(0..100, |conf| {
///             conf.set_arcon_time(ArconTime::Process);
///         })
///         .operator(OperatorBuilder {
///             operator: Arc::new(|| Map::new(|x| x + 10)),
///             state: Arc::new(|_| EmptyState),
///             conf: OperatorConf {
///                 parallelism_strategy: ParallelismStrategy::Static(6),
///                 perf_events,
///                 ..Default::default()
///             },
///         })
///         .to_console()
///         .build();
///     app.start();
///     app.await_termination();
/// }
///```
impl PerfEvents {
    pub fn new() -> PerfEvents {
        PerfEvents { counters: vec![] }
    }

    pub fn add(&mut self, hardware_metric_kind: HardwareCounter) {
        self.counters.push(hardware_metric_kind);
    }
}
