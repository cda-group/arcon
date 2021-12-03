use super::*;
use crate::dataflow::{
    constructor::{ErasedComponent, ErasedComponents},
    dfg::*,
};
use kompact::prelude::SystemField;

/// Deployment
pub struct Deployment {
    /// The Application
    pub application: AssembledApplication,
    pub process_controller_map: FxHashMap<ProcessId, ActorPath>,
    named_path_map: FxHashMap<GlobalNodeId, ActorPath>,
    local_nodes: FxHashMap<GlobalNodeId, ErasedComponent>,
    node_configs: Vec<NodeConfigSet>,
}

impl Deployment {
    pub fn new(application: AssembledApplication) -> Deployment {
        let mut deployment = Deployment {
            application: application.clone(),
            process_controller_map: FxHashMap::default(),
            named_path_map: FxHashMap::default(),
            local_nodes: FxHashMap::default(),
            node_configs: Vec::new(),
        };

        let mut output_channels: Vec<GlobalNodeId> = Vec::new();
        let mut input_channels: &Vec<NodeID>;
        let mut global_node_ids: Vec<GlobalNodeId>;
        let application_id = application.app.conf.application_id;
        for dfg_node in application.app.dfg.graph.iter().rev() {
            let operator_id = dfg_node.get_operator_id();
            input_channels = dfg_node.get_input_channels();
            let config_sets = Deployment::create_node_config_sets(
                application_id,
                operator_id,
                dfg_node.get_node_ids(),
                application.get_process_ids_for_operator(&operator_id),
                input_channels,
                &output_channels,
            );
            deployment.node_configs.append(&mut config_sets.clone());
            output_channels.clear();
            config_sets
                .iter()
                .for_each(|set| output_channels.append(&mut set.get_node_ids()));
        }
        deployment
    }

    /*
    // Create multiple NodeConfig sets with the same input/output set
    fn create_and_insert_node_config_sets(
        &mut self,
        global_node_ids: &Vec<GlobalNodeId>,
        input_channels: &Vec<NodeID>,
        output_channels: &Vec<GlobalNodeId>,
    ) {
        for global_node_id.iter().unique()
        for global_node_id in global_node_ids {
            self.insert_node_config(NodeConfig {
                id: global_node_id.clone(),
                input_channels: input_channels.clone(),
                output_channels: output_channels.clone(),
            });
        }
    } */

    /// Creates global_node_ids from a list of NodeID's and ProcessId's,
    /// Thereby implicitly assigns NodeID's to the ProcessId's
    fn create_node_config_sets(
        application_id: ApplicationId,
        operator_id: OperatorId,
        node_ids: Vec<NodeID>,
        process_ids: Vec<ProcessId>,
        input_channels: &Vec<NodeID>,
        output_channels: &Vec<GlobalNodeId>,
    ) -> Vec<NodeConfigSet> {
        let mut node_config_sets = Vec::new();
        let nodes_per_process = node_ids.len() / process_ids.len();
        let mut node_id_iter = node_ids.iter();
        for process_id in process_ids {
            let mut set = NodeConfigSet::new(
                process_id,
                application_id,
                operator_id,
                input_channels.clone(),
                output_channels.clone(),
            );
            for _ in 0..nodes_per_process {
                if let Some(node_id) = node_id_iter.next() {
                    set.add_node_id(node_id.clone());
                }
            }
            node_config_sets.push(set);
        }
        node_config_sets
    }

    pub fn is_ready(&mut self) -> bool {
        self.process_controller_map.len() >= self.application.app.layout.get_process_count()
    }

    pub fn build_node(&mut self, node_config_set: NodeConfigSet) -> () {
        let (local_receivers, remote_receivers) =
            self.make_receivers(&node_config_set.get_output_channels());
        let dfg_node = self
            .application
            .app
            .dfg
            .get(&node_config_set.operator_id)
            .clone();
        let channel_kind = dfg_node.get_channel_kind().clone();
        match dfg_node.kind {
            DFGNodeKind::Source(mut source_factory) => {
                let sources = Arc::get_mut(&mut source_factory)
                    .expect("Failed to make SourceFactory mutable")
                    .build_source(local_receivers, remote_receivers, &mut self.application);
                self.application.set_source_manager(sources);
                todo!(); // use the source_manager !
            }
            DFGNodeKind::Node(mut constructor) => {
                let components = Arc::get_mut(&mut constructor)
                    .expect("Failed to make NodeFactory mutable")
                    .build_nodes(
                        node_config_set.get_node_ids(),
                        node_config_set.get_input_channels().clone(),
                        local_receivers,
                        remote_receivers,
                        &mut self.application,
                    );
                components.iter().cloned().for_each(|(id, component)| {
                    self.local_nodes.insert(id, component);
                });
            }
        }
    }

    fn make_receivers(
        &self,
        output_channels: &Vec<GlobalNodeId>,
    ) -> (ErasedComponents, Vec<ActorPath>) {
        let mut local = Vec::new();
        let mut remote = Vec::new();
        for output_channel in output_channels {
            if let Some(component) = self.local_nodes.get(output_channel) {
                local.push(component.clone());
            } else {
                remote.push(
                    self.named_path_map
                        .get(output_channel)
                        .expect("No channel found for")
                        .clone(),
                );
            }
        }
        (local, remote)
    }

    fn insert_node_config(&mut self, node_config_set: NodeConfigSet) {
        self.node_configs.push(node_config_set);
    }

    pub fn insert_pid_path(&mut self, pid: ProcessId, path: ActorPath) {
        // Build and insert NamedPaths for the ProcessId
        for node_cfg_set in self.get_node_configs_for_pid(&pid) {
            node_cfg_set.get_node_ids().iter().for_each(|id| {
                self.insert_node_id_path(id, id.to_actor_path(&path.system()));
            });
        }
        assert!(
            self.process_controller_map.insert(pid, path).is_none(),
            "Duplicate pid-path translation inserted"
        );
    }

    pub fn insert_node_id_path(&mut self, global_node_id: &GlobalNodeId, actor_path: ActorPath) {
        assert!(self
            .named_path_map
            .insert(global_node_id.clone(), actor_path.clone())
            .is_none());
    }

    /// Get a Vec of ProcessId and the given
    pub fn get_pid_controller_paths(&self) -> Vec<(&ProcessId, &ActorPath)> {
        self.process_controller_map.iter().collect()
    }

    /// Returns a Vec where each entry is one GlobalNodeId and its corresponding NamedPath
    pub fn get_named_paths(&mut self) -> Vec<(GlobalNodeId, ActorPath)> {
        self.named_path_map
            .iter()
            .map(|(id, path)| (id.clone(), path.clone()))
            .collect()
    }

    /// Returns a Vec of NodeConfigs which belong to the given ProcessId
    pub fn get_node_configs_for_pid(&self, process_id: &ProcessId) -> Vec<NodeConfigSet> {
        let vec = self
            .node_configs
            .iter()
            .filter(|cfg| cfg.process_id == *process_id)
            .cloned()
            .collect();
        vec
    }
}
