use crate::prelude::*;
use application_controller::ApplicationControllerMessage;
use multimap::MultiMap;
use fxhash::FxHashMap;
use process_controller::ProcessControllerMessage;

pub mod application_controller;
pub mod process_controller;

pub type ProcessId = String;
pub type OperatorId = String;
pub type ApplicationId = String;

/// Logical Name, can be derived from a DistributedApplication.
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
    pub fn new(id: GlobalNodeId) -> Self {
        Self {
            id,
            input_channels: Vec::new(),
            output_channels: Vec::new(),
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

/// DistributedApplication
pub struct DistributedApplication {
    /// The Application
    pub application: Application,
    /// The Layout of the Distributed Application
    pub layout: Layout,
}

impl DistributedApplication {
    pub fn build(&mut self) -> DeploymentPlan {
        DeploymentPlan{
            node_map: FxHashMap::default(),
            node_configs: Vec::new(),
        }
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
            node_configs: Vec::new()
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
            .filter(|cfg| {cfg.id().process_id == process_id})
            .cloned().collect()
    }
}

/// Simple Layout Describing the deployment of a distributed pipeline, maps Operator Names to ProcessId's
pub struct Layout {
    /// Maps OperatorName to ProcessId(s): The Operator should be deployed on the ProcessIds it maps to.
    map: MultiMap<OperatorId, ProcessId>,
}

impl Layout {
    pub fn new() -> Self {
        Layout {
            map: MultiMap::new(),
        }
    }
    /// Returns the Operators
    pub fn get_process_ids(&self, operator: &OperatorId) -> Option<&Vec<ProcessId>> {
        self.map.get_vec(operator)
    }

    pub fn insert_mapping(&mut self, operator: &OperatorId, process_id: &ProcessId) {
        self.map.insert(operator.clone(), process_id.clone())
    }
}

// Serialisation
impl Deserialiser<GlobalNodeId> for GlobalNodeId {
    const SER_ID: SerId = 7003;
    fn deserialise(buf: &mut dyn Buf) -> Result<GlobalNodeId, SerError> {
        Ok(GlobalNodeId {
            process_id: String::deserialise(buf)?,
            application_id: String::deserialise(buf)?,
            operator_id: String::deserialise(buf)?,
            node_id: NodeID::from(buf.get_u32()),
        })
    }
}

impl Serialisable for GlobalNodeId {
    fn ser_id(&self) -> SerId {
        Self::SER_ID
    }

    fn size_hint(&self) -> Option<usize> {
        None
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        self.process_id.serialise(buf)?;
        self.application_id.serialise(buf)?;
        self.operator_id.serialise(buf)?;
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
            process_id: "AProcessId".to_string(),
            application_id: "AnApplicationId".to_string(),
            operator_id: "AnOperatorId".to_string(),
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
