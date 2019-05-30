use akka_api::messages::*;
use akka_api::messages::KompactAkkaMsg_oneof_payload::*;
use akka_api::AkkaConnection;
use fnv::FnvHashMap;
use kompact::*;
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct Metric {
    pub task_id: String,
    pub task_avg: u64,
}

pub struct MetricPort;

impl Port for MetricPort {
    type Indication = Metric;
    type Request = Metric;
}

type TaskId = String;
type TaskAvg = u64;

#[derive(ComponentDefinition)]
pub struct TaskManager {
    ctx: ComponentContext<TaskManager>,
    metric_port: ProvidedPort<MetricPort, TaskManager>,
    coordinator: AkkaConnection,
    self_addr: SocketAddr,
    task_metrics: FnvHashMap<TaskId, TaskAvg>,
}

impl TaskManager {
    pub fn new(coordinator: AkkaConnection, self_addr: SocketAddr) -> TaskManager {
        TaskManager {
            ctx: ComponentContext::new(),
            metric_port: ProvidedPort::new(),
            coordinator,
            self_addr,
            task_metrics: FnvHashMap::default(),
        }
    }

    fn handle_msg(&mut self, envelope: KompactAkkaEnvelope) -> crate::error::Result<()> {
        let payload = envelope.msg.unwrap().payload.unwrap();

        match payload {
            snapshotRequest(_) => {}
            hello(_) => {}
            ask(_) => {}
            kompactRegistration(_) => {}
            askReply(_) => {}
        }

        Ok(())
    }
}

impl Provide<ControlPort> for TaskManager {
    fn handle(&mut self, event: ControlEvent) -> () {
        if let ControlEvent::Start = event {
            debug!(self.ctx.log(), "Starting TaskManager");
            let envelope =
                akka_api::registration("task_manager", &self.self_addr, &self.coordinator);
            self.coordinator.path.tell(envelope, self);

            /*
            let timeout = Duration::from_millis(250);
            let timer = self.schedule_periodic(timeout, timeout, |self_c, _| {
                self_c.actor_ref().tell(Box::new(MetricReport {}), self_c);
                //self_c.coordinator_path.tell(Box::new(
            });

            self.report_timer = Some(timer);
            */
        }
    }
}

impl Provide<MetricPort> for TaskManager {
    fn handle(&mut self, metric: Metric) {
        debug!(self.ctx.log(), "Got metric {:?}", metric);
        self.task_metrics.insert(metric.task_id, metric.task_avg);
    }
}

impl Actor for TaskManager {
    fn receive_local(&mut self, _sender: ActorRef, _msg: &Any) {}
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r: Result<KompactAkkaEnvelope, SerError> = akka_api::ProtoSer::deserialise(buf);
            if let Ok(envelope) = r {
                if let Err(e) = self.handle_msg(envelope) {
                    error!(self.ctx.log(), "Error while handling msg {}", e.to_string());
                }
            }
        } else {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use crate::akka_api::util::*;
    use crate::prelude::*;
    use crate::weld::*;
    use state_backend::StateBackend;
    use state_backend::*;
    use kompact::default_components::DeadletterBox;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn task_test() {
        let self_socket_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), 0);
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());

        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let id = String::from("addition");
        let code = String::from("|x:i32| 40 + x");
        let raw_code = generate_raw_module(code.clone(), true).unwrap();
        let priority = 0;
        let mut module = Module::new(id, raw_code, priority, None).unwrap();
        let db = in_memory::InMemory::create("test_storage");

        let ref input: i32 = 10;
        let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
        module
            .add_serializer(code)
            .expect("failed to compile module");
        let serialized_input: Vec<u8> = module.serialize_input(&input, ctx).unwrap();

        let (task, _) = system.create_and_register(move || {
            crate::components::task::Task::new("add".to_string(), module, Arc::new(db), None)
        });

        let _ = system.register_by_alias(&task, "add");

        let coordinator_proxy = String::from("127.0.0.1:2500");
        let coordinator_handler_path = String::from("operator_handler");
        let coordinator_socket_addr = parse_socket_addr(&coordinator_proxy).unwrap();
        let coordinator_path = ActorPath::Named(NamedPath::with_socket(
            Transport::TCP,
            coordinator_socket_addr,
            vec!["coordinator".into()],
        ));

        let coordinator_conn = AkkaConnection {
            path: coordinator_path,
            akka_actor_path: coordinator_handler_path,
            addr_str: coordinator_proxy.to_string(),
        };

        let (task_manager, _) = system
            .create_and_register(move || TaskManager::new(coordinator_conn, self_socket_addr));

        // Connect ports
        let tm_port = task_manager.on_definition(|c| c.metric_port.share());
        let _task_port = task.on_definition(|c| {
            c.manager_port.connect(tm_port);
            c.manager_port.share();
        });

        system.start(&task_manager);
        system.start(&task);

        let mut msg = TaskMsg::new();
        let mut element = Element::new();
        element.set_timestamp(crate::util::get_system_time());
        element.set_id(1);
        element.set_task_id("add".to_string());
        element.set_data(serialized_input);
        msg.set_element(element);

        let task_path = ActorPath::Named(NamedPath::with_system(
            system.system_path(),
            vec!["add".clone().into()],
        ));

        task_path.tell(msg, &system);

        std::thread::sleep(Duration::from_millis(500));
        system.shutdown().expect("fail");
    }
}
