use crate::prelude::*;
use fxhash::FxHashMap;
use kompact::prelude::{ActorPath, NamedPath, SystemPath};
use multimap::MultiMap;
pub(crate) mod application_controller;
pub(crate) mod deployment;
pub(crate) mod process_controller;

pub(crate) use application_controller::*;
pub(crate) use deployment::*;
pub(crate) use process_controller::*;

pub type ProcessId = u32;
pub type OperatorId = usize;
pub type ApplicationId = u32;

/// Logical Name, can be derived from a Deployment.
/// Resolveable to an `ActorPath` during runtime.
#[derive(Debug, Clone, PartialEq, std::cmp::Eq, std::hash::Hash, Copy)]
pub struct GlobalNodeId {
    pub process_id: ProcessId,
    pub application_id: ApplicationId,
    pub operator_id: OperatorId,
    pub node_id: NodeID,
}

impl GlobalNodeId {
    // Helper function to make place-holder GlobalNodeId (used in testing etc.)
    pub fn null() -> GlobalNodeId {
        GlobalNodeId {
            process_id: 0,
            application_id: 0,
            operator_id: 0,
            node_id: NodeID::new(0),
        }
    }
    pub fn to_actor_path(&self, system_path: &SystemPath) -> ActorPath {
        let name = format!(
            "{}_{}_{}_{}",
            self.process_id, self.application_id, self.operator_id, self.node_id.id
        );
        ActorPath::Named(NamedPath::with_system(system_path.clone(), vec![name]))
    }
}

/// NodeConfig Sufficient to start a Node instance of a known Operator
#[derive(Debug, Clone)]
pub struct NodeConfig {
    id: GlobalNodeId,
    input_channels: Vec<NodeID>,
    output_channels: Vec<GlobalNodeId>,
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
}

/// NodeConfigSet Sufficient config to start a Node instance of a known Operator
#[derive(Debug, Clone)]
pub struct NodeConfigSet {
    pub process_id: ProcessId,
    pub application_id: ApplicationId,
    pub operator_id: OperatorId,
    pub node_ids: Vec<NodeID>,
    input_channels: Vec<NodeID>,
    output_channels: Vec<GlobalNodeId>,
}

impl NodeConfigSet {
    pub fn new(
        process_id: ProcessId,
        application_id: ApplicationId,
        operator_id: OperatorId,
        input_channels: Vec<NodeID>,
        output_channels: Vec<GlobalNodeId>,
    ) -> NodeConfigSet {
        Self {
            process_id,
            application_id,
            operator_id,
            node_ids: Vec::new(),
            input_channels,
            output_channels,
        }
    }

    pub fn add_node_id(&mut self, node_id: NodeID) {
        self.node_ids.push(node_id);
    }

    pub fn get_node_ids(&self) -> Vec<GlobalNodeId> {
        self.node_ids
            .iter()
            .map(|node_id| GlobalNodeId {
                process_id: self.process_id,
                application_id: self.application_id,
                operator_id: self.operator_id,
                node_id: node_id.clone(),
            })
            .collect()
    }

    pub fn add_input_channel(&mut self, node_id: NodeID) {
        self.input_channels.push(node_id);
    }

    pub fn add_output_channel(&mut self, node_id: GlobalNodeId) {
        self.output_channels.push(node_id);
    }

    pub fn get_output_channels(&self) -> &Vec<GlobalNodeId> {
        &self.output_channels
    }

    pub fn get_input_channels(&self) -> &Vec<NodeID> {
        &self.input_channels
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
            operator_id: buf.get_u32() as usize,
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
        buf.put_u32(self.operator_id as u32);
        buf.put_u32(self.node_id.id);
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

impl Deserialiser<NodeConfigSet> for NodeConfigSet {
    const SER_ID: SerId = 7001;
    fn deserialise(buf: &mut dyn Buf) -> Result<NodeConfigSet, SerError> {
        let process_id = buf.get_u32();
        let application_id = buf.get_u32();
        let operator_id = buf.get_u32() as usize;
        let mut set = NodeConfigSet::new(
            process_id,
            application_id,
            operator_id,
            Vec::new(),
            Vec::new(),
        );

        let node_id_length = buf.get_u32();
        for _ in 0..node_id_length {
            set.add_node_id(NodeID::new(buf.get_u32()));
        }

        let input_channels_length = buf.get_u32();
        for _ in 0..input_channels_length {
            set.add_input_channel(NodeID::from(buf.get_u32()));
        }

        let output_channels_length = buf.get_u32();
        for _ in 0..output_channels_length {
            set.add_output_channel(GlobalNodeId::deserialise(buf)?);
        }
        Ok(set)
    }
}

impl Serialisable for NodeConfigSet {
    fn ser_id(&self) -> SerId {
        Self::SER_ID
    }

    fn size_hint(&self) -> Option<usize> {
        None
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_u32(self.process_id);
        buf.put_u32(self.application_id);
        buf.put_u32(self.operator_id as u32);

        buf.put_u32(self.node_ids.len() as u32);
        for node_id in &self.node_ids {
            buf.put_u32(node_id.id);
        }

        buf.put_u32(self.input_channels.len() as u32);
        for &input_channel in &self.input_channels {
            buf.put_u32(input_channel.id);
        }

        buf.put_u32(self.output_channels.len() as u32);
        for node_id in self.output_channels.iter() {
            node_id.serialise(buf)?;
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

    fn dummy_node_config() -> NodeConfigSet {
        let mut cfg = NodeConfigSet::new(0, 0, 0, Vec::new(), Vec::new());
        cfg.add_input_channel(NodeID::new(4));
        cfg.add_input_channel(NodeID::new(8));
        cfg.add_input_channel(NodeID::new(15));
        cfg.add_input_channel(NodeID::new(16));
        cfg.add_input_channel(NodeID::new(23));
        cfg.add_input_channel(NodeID::new(42));
        cfg.add_output_channel(GlobalNodeId::null());
        cfg.add_output_channel(GlobalNodeId::null());
        cfg
    }

    #[test]
    fn node_config_serialiser_test() {
        let mut buffer = BytesMut::new();
        let node_config = dummy_node_config();

        node_config.serialise(&mut buffer).ok();
        let deserialised = NodeConfigSet::deserialise(&mut buffer).unwrap();
        assert_eq!(deserialised.get_node_ids(), node_config.get_node_ids());
        assert_eq!(deserialised.input_channels, node_config.input_channels);
        assert_eq!(deserialised.output_channels, node_config.output_channels);
    }
}
