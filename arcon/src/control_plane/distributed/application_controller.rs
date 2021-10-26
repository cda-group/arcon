use super::*;
use fxhash::FxHashMap;

/// Global coordinator of a Pipeline, coordinates all the processes' (hosts) executing the application/pipeline.
#[derive(ComponentDefinition)]
pub(crate) struct ApplicationController {
    /// Component context
    ctx: ComponentContext<Self>,
    /// The Application
    application: DistributedApplication,
    /// The number of processes which the Application Requires
    remaining_check_ins: u32,
    /// ActorPath's to the ProcessControllers
    process_controllers: FxHashMap<ProcessId, ActorPath>,
}

impl ApplicationController {
    pub fn new(application: DistributedApplication, expected_processes: u32) -> Self {
        ApplicationController {
            ctx: ComponentContext::uninitialised(),
            application,
            remaining_check_ins: expected_processes,
            process_controllers: FxHashMap::default(),
        }
    }

    fn handle_check_in(&mut self, pid: ProcessId, path: ActorPath) {
        if self.process_controllers.insert(pid, path).is_none() {
            self.remaining_check_ins -= 1
        };
        if self.remaining_check_ins == 0 {
            self.start_application();
        }
    }

    fn start_application(&mut self) {
        let distribution_plan = self.application.build();
        for (pid, path) in self.process_controllers.iter() {
            // self.application.get_node_configs();
            path.tell_serialised(
                ProcessControllerMessage::CreateOperators(
                    Vec::new(),
                    Vec::new()
                ), 
                self)
                .ok();
        }
    }
}

impl ComponentLifecycle for ApplicationController {
    fn on_start(&mut self) -> Handled {
        // Do anything?
        Handled::Ok
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ApplicationControllerMessage {
    /// CheckIn sent by ProcessControllers format: (ProcessId, ApplicationName)
    CheckIn(ProcessId),
    /// Created Operators, response sent by ProcessControllers
    CreatedOperators(Vec<NodeID>),
}

impl NetworkActor for ApplicationController {
    type Deserialiser = ApplicationControllerMessage;
    type Message = ApplicationControllerMessage;

    fn receive(&mut self, sender: Option<ActorPath>, msg: Self::Message) -> Handled {
        match msg {
            ApplicationControllerMessage::CheckIn(pid) => {
                info!(self.ctx.log(), "CheckIn Received: {:?}", pid);
                if let Some(process_controller) = sender {
                    self.handle_check_in(pid, process_controller);
                }
            }
            ApplicationControllerMessage::CreatedOperators(node_id_vec) => {
                info!(
                    self.ctx.log(),
                    "CreatedOperators Received {:?}", node_id_vec
                );
            }
        }
        Handled::Ok
    }
}

// Serialisation
const CHECK_IN_ID: u8 = 0;
const CREATED_OPERATORS_ID: u8 = 1;

impl Deserialiser<ApplicationControllerMessage> for ApplicationControllerMessage {
    const SER_ID: SerId = 7000;

    fn deserialise(buf: &mut dyn Buf) -> Result<ApplicationControllerMessage, SerError> {
        match buf.get_u8() {
            CREATED_OPERATORS_ID => {
                let mut node_vec = Vec::new();
                for _ in 0..buf.get_u32() {
                    node_vec.push(NodeID::from(buf.get_u32()));
                }
                Ok(ApplicationControllerMessage::CreatedOperators(node_vec))
            }
            CHECK_IN_ID => Ok(ApplicationControllerMessage::CheckIn(String::deserialise(
                buf,
            )?)),
            _ => Err(SerError::InvalidData(
                "Unable to Deserialise ApplicationControllerMessage".to_string(),
            )),
        }
    }
}

impl Serialisable for ApplicationControllerMessage {
    fn ser_id(&self) -> u64 {
        Self::SER_ID
    }
    fn size_hint(&self) -> Option<usize> {
        match self {
            ApplicationControllerMessage::CreatedOperators(node_vec) => None,
            ApplicationControllerMessage::CheckIn(pid) => None,
        }
    }
    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            ApplicationControllerMessage::CreatedOperators(node_vec) => {
                buf.put_u8(CREATED_OPERATORS_ID);
                buf.put_u32(node_vec.len() as u32);
                for node_id in node_vec {
                    buf.put_u32(node_id.id);
                }
            }
            ApplicationControllerMessage::CheckIn(pid) => {
                pid.serialise(buf)?;
            }
        }
        Ok(())
    }
    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
