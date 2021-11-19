use super::*;
use fxhash::FxHashMap;

/// Local coordinator of all executors within a process (host)
#[derive(ComponentDefinition)]
pub(crate) struct ProcessController {
    /// Component context
    ctx: ComponentContext<Self>,
    /// The ProcessId of the ProcessController System
    pid: ProcessId,
    /// ActorPaths to the Operators Spawned by the ProcessController
    operator_paths: Vec<ActorPath>,
    /// ActorPaths to the Sources,
    sources_paths: Vec<ActorPath>,
    /// Global Path Map, mapping NodeID's to NamedPaths
    named_path_map: FxHashMap<NodeID, NamedPath>,
    /// ApplicationController path
    application_controller: ActorPath,
    /// The Application 
    application: AssembledApplication,
}

impl ProcessController {
    pub fn new(
        application: AssembledApplication,
    ) -> Self {
        ProcessController {
            ctx: ComponentContext::uninitialised(),
            pid: application.app.process_id,
            application_controller: application.get_application_controller().expect("No Application Controller ActorPath"),
            operator_paths: Vec::new(),
            sources_paths: Vec::new(),
            named_path_map: FxHashMap::default(),
            application,
        }
    }

    pub fn create_operators(&self, node_map: Vec<(GlobalNodeId, ActorPath)>, config_vec: Vec<NodeConfig>) {
        for config in config_vec {
            /*
            if let Some(builder) = self.application.get_operator_builder(config.id) {
                builder(config, node_map);
            }
            */
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ProcessControllerMessage {
    /// Contains GlobalNodeId -> ActorPath and OperatorName -> NodeConfig
    CreateOperators(Vec<(GlobalNodeId, ActorPath)>, Vec<NodeConfig>),
    StartSources,
}

impl ComponentLifecycle for ProcessController {
    fn on_start(&mut self) -> Handled {
        info!(self.ctx.log(), "starting ProcessController with path {:?}, telling ApplicationController {:?}", 
            self.ctx.actor_path(),
            self.application_controller);
        self.application_controller
            .tell_serialised(
                ApplicationControllerMessage::CheckIn(self.pid.clone()),
                self,
            )
            .ok();
        Handled::Ok
    }
}

impl NetworkActor for ProcessController {
    type Deserialiser = ProcessControllerMessage;
    type Message = ProcessControllerMessage;
    fn receive(&mut self, source: Option<ActorPath>, msg: Self::Message) -> Handled {
        match msg {
            ProcessControllerMessage::CreateOperators(node_map, config_vec) => {
                info!(
                    self.ctx.log(),
                    "CreateOperators Received: {:?}, {:?}", node_map, config_vec
                );
                self.create_operators(node_map, config_vec);
            }
            ProcessControllerMessage::StartSources => {
                info!(self.ctx.log(), "StartSources Received");
            }
        }

        Handled::Ok
    }
}

// Serialisation
const CREATE_OPERATORS_ID: u8 = 0;
const START_SOURCES_ID: u8 = 1;

impl Deserialiser<ProcessControllerMessage> for ProcessControllerMessage {
    const SER_ID: SerId = 7001;

    fn deserialise(buf: &mut dyn Buf) -> Result<ProcessControllerMessage, SerError> {
        match buf.get_u8() {
            CREATE_OPERATORS_ID => {
                let mut node_vec = Vec::new();
                let mut config_vec = Vec::new();

                let node_vec_length = buf.get_u32();
                for _ in 0..node_vec_length {
                    let node_id = GlobalNodeId::deserialise(buf)?;
                    let path = ActorPath::deserialise(buf)?;
                    node_vec.push((node_id, path));
                }
                let config_vec_length = buf.get_u32();
                for _ in 0..config_vec_length {
                    let node_config = NodeConfig::deserialise(buf)?;
                    config_vec.push(node_config);
                }
                Ok(ProcessControllerMessage::CreateOperators(
                    node_vec, config_vec,
                ))
            }
            START_SOURCES_ID => Ok(ProcessControllerMessage::StartSources),
            _ => Err(SerError::InvalidData(
                "Unable to Deserialise ProcessControllerMessage".to_string(),
            )),
        }
    }
}

impl Serialisable for ProcessControllerMessage {
    fn ser_id(&self) -> u64 {
        Self::SER_ID
    }
    fn size_hint(&self) -> Option<usize> {
        match self {
            ProcessControllerMessage::CreateOperators(node_map, config_vec) => None,
            ProcessControllerMessage::StartSources => Some(1),
        }
    }
    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            ProcessControllerMessage::CreateOperators(node_map, config_vec) => {
                buf.put_u8(CREATE_OPERATORS_ID);
                buf.put_u32(node_map.len() as u32);
                for (node_id, actor_path) in node_map {
                    node_id.serialise(buf)?;
                    actor_path.serialise(buf)?;
                }
                buf.put_u32(config_vec.len() as u32);
                for config in config_vec {
                    config.serialise(buf)?;
                }
            }
            ProcessControllerMessage::StartSources => {
                buf.put_u8(START_SOURCES_ID);
            }
        }
        Ok(())
    }
    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
