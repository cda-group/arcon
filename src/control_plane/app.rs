use kompact::prelude::ActorPath;
use std::{collections::HashMap, str::FromStr};

pub type ProcessID = usize;
pub type AppID = String;

#[derive(prost::Message, Clone)]
pub struct AppRegistration {
    #[prost(string)]
    pub name: String,
    #[prost(message, repeated)]
    pub arcon_pids: Vec<u64>,
    #[prost(message, repeated)]
    pub sources: Vec<String>,
    #[prost(uint64)]
    pub pid: u64,
}

/// Information of a running Arcon application
///
/// An Arcon App may consist of multiple Arcon processes if running in a distributed fashion
/// So when we refer to arcon pids, we mean the id of such process, and not actual OS PID.
///
/// Example: App { p1, p2, p3, ...}
pub struct App {
    /// Name of the Arcon app
    ///
    /// Note that this name must be unique
    pub name: String,
    /// ActorPath's to the sources of the app
    source_managers: HashMap<ProcessID, ActorPath>,
}

impl App {
    pub fn new(app_reg: AppRegistration) -> Self {
        let mut source_managers = HashMap::new();
        for (pid, source) in app_reg.sources.into_iter().enumerate() {
            let path = ActorPath::from_str(&source).expect("failed to parse path");
            source_managers.insert(pid, path);
        }
        Self {
            name: app_reg.name,
            source_managers,
        }
    }
    pub fn source_managers(&self) -> Vec<ActorPath> {
        self.source_managers.values().cloned().collect()
    }
}
