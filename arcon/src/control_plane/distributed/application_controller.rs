use super::*;

/// Global coordinator of a Pipeline, coordinates all the processes' (hosts) executing the application/pipeline.
#[derive(ComponentDefinition)]
pub(crate) struct ApplicationController {
    /// Component context
    ctx: ComponentContext<Self>,
    // The Application
    application: AssembledApplication,
}

impl ApplicationController {
    pub fn new(application: AssembledApplication, expected_processes: u32) -> Self {
        ApplicationController {
            ctx: ComponentContext::uninitialised(),
            application,
        }
    }

    fn handle_check_in(&mut self, pid: ProcessId, path: ActorPath) {
        /*
        self.application.insert_pid_path(pid, path);
        if self.application.is_ready() {
            self.start_application();
        }
        */
    }

    fn start_application(&mut self) {
        /*
        let node_id_paths = self.application.get_named_paths();
        for (pid, path) in self.application.get_pid_controller_paths() {
            info!(self.ctx.log(), "Sending CreateOperators command to {:?}", path);
            path.tell_serialised(
                ProcessControllerMessage::CreateOperators(
                    node_id_paths.clone(), self.application.get_node_configs_for_pid(pid)
                ),
                self
            ).ok();
        } */
    }
}

impl ComponentLifecycle for ApplicationController {
    fn on_start(&mut self) -> Handled {
        // Do anything?
        info!(
            self.ctx.log(),
            "Starting ApplicationController {:?}",
            self.ctx.actor_path()
        );
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
        info!(self.ctx.log(), "Received msg: {:?}", msg);
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
            CHECK_IN_ID => Ok(ApplicationControllerMessage::CheckIn(buf.get_u32())),
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
                buf.put_u8(CHECK_IN_ID);
                buf.put_u32(*pid);
            }
        }
        Ok(())
    }
    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
