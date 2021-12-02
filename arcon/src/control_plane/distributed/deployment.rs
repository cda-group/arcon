use super::*;
use crate::dataflow::{
    constructor::{ErasedComponents},
    dfg::*,
};
use kompact::prelude::SystemField;

/// Deployment
pub struct Deployment {
    /// The Application
    pub application: AssembledApplication,
    pub process_controller_map: FxHashMap<ProcessId, ActorPath>,
    named_path_map: FxHashMap<GlobalNodeId, ActorPath>,
    local_nodes: FxHashMap<GlobalNodeId, ErasedComponents>,
    node_configs: Vec<NodeConfig>,
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
        let mut input_channels: Vec<NodeID>;
        let mut global_node_ids: Vec<GlobalNodeId>;
        let application_id = application.app.conf.application_id;
        for dfg_node in application.app.dfg.graph.iter().rev() {
            let operator_id = dfg_node.get_operator_id();
            global_node_ids = Deployment::create_global_node_ids(
                dfg_node.get_node_ids(),
                operator_id,
                application.get_process_ids_for_operator(&operator_id),
                application_id,
            );
            input_channels = dfg_node.get_input_channels();
            deployment.create_and_insert_node_config_set(
                &global_node_ids,
                &input_channels,
                &output_channels,
            );
            // Create the output_channels for the next iteration
            output_channels = global_node_ids.clone();
        }
        deployment
    }

    // Create multiple node configs with the same input/output set
    fn create_and_insert_node_config_set(
        &mut self,
        global_node_ids: &Vec<GlobalNodeId>,
        input_channels: &Vec<NodeID>,
        output_channels: &Vec<GlobalNodeId>,
    ) {
        for global_node_id in global_node_ids {
            self.insert_node_config(NodeConfig {
                id: global_node_id.clone(),
                input_channels: input_channels.clone(),
                output_channels: output_channels.clone(),
            });
        }
    }

    /// Creates global_node_ids from a list of NodeID's and ProcessId's,
    /// Thereby implicitly assigns NodeID's to the ProcessId's
    fn create_global_node_ids(
        node_ids: Vec<NodeID>,
        operator_id: OperatorId,
        process_ids: Vec<ProcessId>,
        application_id: ApplicationId,
    ) -> Vec<GlobalNodeId> {
        let mut global_node_ids = Vec::new();
        let nodes_per_process = node_ids.len() / process_ids.len();
        let mut node_id_iter = node_ids.iter();
        for process_id in process_ids {
            for _ in 0..nodes_per_process {
                if let Some(node_id) = node_id_iter.next() {
                    global_node_ids.push(GlobalNodeId {
                        process_id,
                        application_id,
                        operator_id,
                        node_id: node_id.clone(),
                    });
                }
            }
        }
        global_node_ids
    }

    pub fn is_ready(&mut self) -> bool {
        self.process_controller_map.len() >= self.application.app.layout.get_process_count()
    }

    pub fn build_node(&mut self, node_config: NodeConfig) -> () {
        let (local_receivers, remote_receivers) = self.make_receivers(&node_config);
        let dfg_node = self
            .application
            .app
            .dfg
            .get(&node_config.id().operator_id)
            .clone();
        let channel_kind = dfg_node.get_channel_kind().clone();
        match dfg_node.kind {
            DFGNodeKind::Source(channel_kind, source_manager_cons) => {
                let sources = source_manager_cons.build(
                    local_receivers,
                    remote_receivers,
                    channel_kind,
                    &mut self.application,
                );

                // todo!(); // use the source_manager
            }
            DFGNodeKind::Node(manager_cons) => {
                /*let components = manager_cons.build_nodes(
                    node_config.id.node_id,
                    node_config.input_channels,
                    local_receivers,
                    remote_receivers,
                    channel_kind,
                    &mut self.application,
                );
                self.local_nodes.insert(node_config.id, component);
                */
                todo!(); // use the node_manager
            }
        }
    }

    fn make_receivers(&self, node_config: &NodeConfig) -> (ErasedComponents, Vec<ActorPath>) {
        let mut local = Vec::new();
        let mut remote = Vec::new();
        for output_channel in node_config.get_output_channels() {
            if let Some(components) = self.local_nodes.get(output_channel) {
                local = components.clone();
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

    fn insert_node_config(&mut self, node_config: NodeConfig) {
        self.node_configs.push(node_config);
    }

    pub fn insert_pid_path(&mut self, pid: ProcessId, path: ActorPath) {
        // Build and insert NamedPaths for the ProcessId
        for node_cfg in self.get_node_configs_for_pid(&pid) {
            self.insert_node_id_path(node_cfg.id(), node_cfg.id().to_actor_path(&path.system()));
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
    pub fn get_node_configs_for_pid(&self, process_id: &ProcessId) -> Vec<NodeConfig> {
        let vec = self
            .node_configs
            .iter()
            .filter(|cfg| cfg.id().process_id == *process_id)
            .cloned()
            .collect();
        vec
    }
}
