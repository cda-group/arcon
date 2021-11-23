use crate::prelude::*;

// use application_controller::ApplicationControllerMessage;
use fxhash::FxHashMap;
use multimap::MultiMap;
// use process_controller::ProcessControllerMessage;
pub(crate) mod application_controller;
pub(crate) mod process_controller;

pub(crate) use application_controller::*;
pub(crate) use process_controller::*;

pub type ProcessId = u32;
pub type OperatorId = u32;
pub type ApplicationId = u32;

/// Logical Name, can be derived from a Deployment.
/// Resolveable to an `ActorPath` during runtime.
#[derive(Debug, Clone, PartialEq)]
pub struct GlobalNodeId {
    pub process_id: ProcessId,
    pub application_id: ApplicationId,
    pub operator_id: OperatorId,
    pub node_id: NodeID,
}

/// NodeConfig Sufficient to start a Node instance of a known Operator
#[derive(Debug, Clone)]
pub struct NodeConfig {
    id: GlobalNodeId,
    input_channels: Vec<NodeID>,
    output_channels: Vec<(KeyRange, NodeID)>,
}

impl NodeConfig {
    pub fn new(id: GlobalNodeId, input_channels: Option<Vec<NodeID>>, output_channels: Option<Vec<(KeyRange, NodeID)>>) -> Self {
        Self {
            id,
            input_channels: input_channels.
            output_channels,
        }
    }

    pub fn id(&self) -> &GlobalNodeId {
        &self.id
    }

    pub fn add_input_channel(&mut self, node_id: NodeID) {
        self.input_channels.push(node_id);
    }

    pub fn add_output_channel(&mut self, range: KeyRange, node_id: NodeID) {
        self.output_channels.push((range, node_id));
    }
}

/// Deployment
pub struct Deployment {
    /// The Application
    // pub application: AssembledApplication,
    pub process_controller_map: FxHashMap<ProcessId, ActorPath>,
    node_id_paths: Vec<(GlobalNodeId, ActorPath)>,
    node_configs: Vec<NodeConfig>,
}

impl Deployment {
    pub fn new(application: &AssembledApplication) -> Deployment {
        let deployment = Deployment {
            process_controller_map: FxHashMap::default(),
            node_id_paths: Vec::new(),
            node_configs: Vec::new(),
        };
        let layout = &application.app.layout.expect("No layout found when building deployment");
        
        // Enter build loop: 
        let mut output_channels: Option<Vec<(KeyRange, NodeID)>> = None;
        let mut input_channels: Option<Vec<NodeID>> = None;
        let mut global_node_id: Option<Vec<GlobalNodeId>> = None;
        
        
        for dfg_node in application.app.dfg.graph.into_iter().rev() {
            if let Some(id_vec) = global_node_id.take() {
                let in = input_channels.take();
                let out = output_channels.take();
                for id in id_vec {
                    deployment.insert_node_config(NodeConfig::new(id, in.clone(), out.clone()))
                }
                
                // Create output channels from the id_vec
                // let output_channels_vec = Shard::get_shards(id_vec.length());
                
            }
            input_channels = Some(dfg_node.get_input_channels());
            // set the list of global_node_id ...
            // need to use layout...
            /*
            match dfg_node.kind {
                DFGNodeKind::Source(channel_kind, source_manager_cons) => {
                    let nodes = target_nodes.take().unwrap();
                    let source_manager =
                        source_manager_cons(nodes, channel_kind, &mut self.ctx.app);

                    self.ctx.app.source_manager = Some(source_manager);
                }
                DFGNodeKind::Node(manager_cons) => {
                    let (channel_kind, components) = {
                        match target_nodes {
                            Some(comps) => (dfg_node.channel_kind, comps),
                            None => {
                                // At the end of the graph....
                                if self.ctx.app.debug_node_enabled() {
                                    let node: DebugNode<IN> = DebugNode::new();
                                    self.ctx.app.create_debug_node(node);
                                }

                                match self.ctx.app.abstract_debug_node {
                                    Some(ref debug_node) => {
                                        (ChannelKind::Forward, vec![debug_node.clone()])
                                    }
                                    None => (
                                        if self.ctx.console_output {
                                            ChannelKind::Console
                                        } else {
                                            ChannelKind::Mute
                                        },
                                        vec![],
                                    ),
                                }
                            }
                        }
                    };

                    // Create expected incoming channels ids
                    let in_channels: Vec<NodeID> = (0..dfg_node.ingoing_channels)
                        .map(|i| NodeID::new(i as u32))
                        .collect();

                    let nodes =
                        manager_cons(in_channels, components, channel_kind, &mut self.ctx.app);

                    target_nodes = Some(nodes);
                }
            }
            */
        }
        // Last element of the iterator must also be inserted
        if let Some(id) = global_node_id.take() {
                deployment.insert_node_config(NodeConfig::new(id, input_channels.take(), output_channels.take()))
        }
        deployment
    }

    fn insert_node_config(&mut self, node_config: NodeConfig) {
        self.node_configs.push(node_config);
    }
    // pub fn get_operator_builder(&self, ) -> dyn Fn(NodeConfig) -> () {}

    pub fn is_ready(&mut self) -> bool {
        true
        /*
        if self.process_controller_map.len() >= self.application.app.layout.get_process_count() {
            self.build_node_configs();
            true
        } else {
            false
        } */
    }

    pub fn insert_pid_path(&mut self, pid: ProcessId, path: ActorPath) {
        assert!(
            self.process_controller_map.insert(pid, path).is_none(),
            "Duplicate pid-path translation inserted"
        );
    }

    /// Get a Vec of ProcessId and the given
    pub fn get_pid_controller_paths(&self) -> Vec<(&ProcessId, &ActorPath)> {
        self.process_controller_map.iter().collect()
    }

    /// Returns a Vec where each entry is one GlobalNodeId and its corresponding NamedPath
    pub fn get_named_paths(&mut self) -> Vec<(GlobalNodeId, ActorPath)> {
        self.node_id_paths.clone()
    }

    /// Returns a Vec of (OperatorId, NodeConfig) which should be deployed to the given ProcessId
    pub fn get_node_configs_for_pid(&self, process_id: &ProcessId) -> Vec<NodeConfig> {
        let vec = self
            .node_configs
            .iter()
            .filter(|cfg| cfg.id().process_id == *process_id)
            .cloned()
            .collect();
        vec
    }

    /// Iterates over the given application and constructs a list of Operators and their configs
    fn build_node_configs(&mut self) {
        // TODO: Build all node configs
        /*
        for dfg_node in self.dfg.graph.into_iter().rev() {
            match dfg_node.kind {
                DFGNodeKind::Source(channel_kind, source_manager_cons) => {
                    // let nodes = target_nodes.take().unwrap();
                    // let source_manager =
                    //    source_manager_cons(nodes, channel_kind, &mut self.app);

                    // self.app.source_manager = Some(source_manager);
                }
                DFGNodeKind::Node(manager_cons) => {

                }
            }
        }
        */
    }
}

pub struct DeploymentPlan {
    node_map: FxHashMap<GlobalNodeId, ActorPath>,
    node_configs: Vec<NodeConfig>,
}

impl DeploymentPlan {
    pub fn new() -> Self {
        DeploymentPlan {
            node_map: FxHashMap::default(),
            node_configs: Vec::new(),
        }
    }
    pub fn get_node_map_vec(&self) -> Vec<(GlobalNodeId, ActorPath)> {
        let mut ret = Vec::new();
        for (id, path) in self.node_map.iter() {
            ret.push((id.clone(), path.clone()));
        }
        ret
    }

    pub fn get_node_configs(&self, process_id: ProcessId) -> Vec<NodeConfig> {
        self.node_configs
            .iter()
            .filter(|cfg| cfg.id().process_id == process_id)
            .cloned()
            .collect()
    }
}

/// Simple Layout Describing the deployment of a distributed pipeline, maps Operator Names to ProcessId's
#[derive(Clone)]
pub struct Layout {
    /// Maps OperatorName to ProcessId(s): The Operator should be deployed on the ProcessIds it maps to.
    map: MultiMap<OperatorId, ProcessId>,
    process_count: usize,
}

impl Layout {
    pub fn new() -> Self {
        Layout {
            map: MultiMap::new(),
            process_count: 0,
        }
    }
    /// Returns the Operators
    pub fn get_process_ids(&self, operator: &OperatorId) -> Option<&Vec<ProcessId>> {
        self.map.get_vec(operator)
    }

    pub fn get_process_count(&self) -> usize {
        self.process_count
    }

    pub fn insert_mapping(&mut self, operator: &OperatorId, process_id: &ProcessId) {
        self.map.insert(operator.clone(), process_id.clone());
        self.update_process_count();
    }

    fn update_process_count(&mut self) {
        let mut pid_vec: Vec<&ProcessId> =
            self.map.iter().map(|(_, process_id)| process_id).collect();
        pid_vec.sort();
        pid_vec.dedup();
        self.process_count = pid_vec.len();
    }
}

// Serialisation
impl Deserialiser<GlobalNodeId> for GlobalNodeId {
    const SER_ID: SerId = 7003;
    fn deserialise(buf: &mut dyn Buf) -> Result<GlobalNodeId, SerError> {
        Ok(GlobalNodeId {
            process_id: buf.get_u32(),
            application_id: buf.get_u32(),
            operator_id: buf.get_u32(),
            node_id: NodeID::from(buf.get_u32()),
        })
    }
}

impl Serialisable for GlobalNodeId {
    fn ser_id(&self) -> SerId {
        Self::SER_ID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(4 * 4)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_u32(self.process_id);
        buf.put_u32(self.application_id);
        buf.put_u32(self.operator_id);
        buf.put_u32(self.node_id.id);
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

impl Deserialiser<NodeConfig> for NodeConfig {
    const SER_ID: SerId = 7001;
    fn deserialise(buf: &mut dyn Buf) -> Result<NodeConfig, SerError> {
        let id = GlobalNodeId::deserialise(buf)?;
        let mut config = NodeConfig::new(id);
        let input_channels_length = buf.get_u32();
        for _ in 0..input_channels_length {
            config.add_input_channel(NodeID::from(buf.get_u32()));
        }
        let output_channels_length = buf.get_u32();
        for _ in 0..output_channels_length {
            config.add_output_channel(
                KeyRange::new(buf.get_u64(), buf.get_u64()),
                NodeID::from(buf.get_u32()),
            )
        }
        Ok(config)
    }
}

impl Serialisable for NodeConfig {
    fn ser_id(&self) -> SerId {
        Self::SER_ID
    }

    fn size_hint(&self) -> Option<usize> {
        None
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        self.id.serialise(buf)?;
        buf.put_u32(self.input_channels.len() as u32);
        for &input_channel in &self.input_channels {
            buf.put_u32(input_channel.id);
        }
        buf.put_u32(self.output_channels.len() as u32);
        for (range, node_id) in self.output_channels.iter() {
            buf.put_u64(range.start);
            buf.put_u64(range.end);
            buf.put_u32(node_id.id);
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use bytes::BytesMut;

    fn dummy_global_id() -> GlobalNodeId {
        GlobalNodeId {
            process_id: 0,
            application_id: 0,
            operator_id: 0,
            node_id: NodeID::new(0),
        }
    }

    fn dummy_node_config() -> NodeConfig {
        let mut cfg = NodeConfig::new(dummy_global_id());
        cfg.add_input_channel(NodeID::new(4));
        cfg.add_input_channel(NodeID::new(8));
        cfg.add_input_channel(NodeID::new(15));
        cfg.add_input_channel(NodeID::new(16));
        cfg.add_input_channel(NodeID::new(23));
        cfg.add_input_channel(NodeID::new(42));
        cfg.add_output_channel(KeyRange::new(0, 32), NodeID::new(1337));
        cfg.add_output_channel(KeyRange::new(33, 64), NodeID::new(666));
        cfg
    }

    #[test]
    fn node_config_serialiser_test() {
        let mut buffer = BytesMut::new();
        let node_config = dummy_node_config();

        node_config.serialise(&mut buffer).ok();
        let deserialised = NodeConfig::deserialise(&mut buffer).unwrap();
        assert_eq!(deserialised.id(), node_config.id());
        assert_eq!(deserialised.input_channels, node_config.input_channels);
        assert_eq!(deserialised.output_channels, node_config.output_channels);
    }
}
