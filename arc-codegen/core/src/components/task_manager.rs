use akka_api::messages::*;
use akka_api::AkkaConnection;
use fnv::FnvHashMap;
use kompact::*;
use std::net::SocketAddr;

#[derive(Clone, Debug)]
struct Metric {
    task_id: String,
    task_avg: u64,
}

struct MetricPort;

impl Port for MetricPort {
    type Indication = Metric;
    type Request = Metric;
}

type TaskId = String;
type TaskAvg = u64;

#[derive(ComponentDefinition)]
pub struct TaskManager {
    ctx: ComponentContext<TaskManager>,
    metric_port: RequiredPort<MetricPort, TaskManager>,
    coordinator: AkkaConnection,
    self_addr: SocketAddr,
    task_metrics: FnvHashMap<TaskId, TaskAvg>,
}

impl TaskManager {
    pub fn new(coordinator: AkkaConnection, self_addr: SocketAddr) -> TaskManager {
        TaskManager {
            ctx: ComponentContext::new(),
            metric_port: RequiredPort::new(),
            coordinator,
            self_addr,
            task_metrics: FnvHashMap::default(),
        }
    }

    fn handle_msg(&mut self, envelope: KompactAkkaEnvelope) -> crate::error::Result<()> {
        let payload = envelope.msg.unwrap().payload.unwrap();

        match payload {
            KompactAkkaMsg_oneof_payload::snapshotRequest(_) => {}
            KompactAkkaMsg_oneof_payload::hello(_) => {}
            KompactAkkaMsg_oneof_payload::ask(_) => {}
            KompactAkkaMsg_oneof_payload::kompactRegistration(_) => {}
            KompactAkkaMsg_oneof_payload::askReply(_) => {}
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

impl Require<MetricPort> for TaskManager {
    fn handle(&mut self, metric: Metric) {
        debug!(self.ctx.log(), "Got metric {:?}", metric);
        self.task_metrics.insert(metric.task_id, metric.task_avg);
        for (key, val) in self.task_metrics.iter() {
            println!("key: {} val: {}", key, val);
        }
    }
}

impl Actor for TaskManager {
    fn receive_local(&mut self, _sender: ActorRef, _msg: Box<Any>) {}
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
mod tests {
    use super::*;
    use crate::akka_api::util::*;
    use crate::module::{Module, ModuleRun};
    use crate::prelude::*;
    use backend::state_backend::StateBackend;
    use backend::*;
    use kompact::default_components::DeadletterBox;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone, Copy)]
    struct MetricReport {}

    // Example Task setup

    #[derive(ComponentDefinition)]
    pub struct Task {
        ctx: ComponentContext<Task>,
        report_timer: Option<ScheduledTimer>,
        manager_port: ProvidedPort<MetricPort, Task>,
        udf: Module,
        udf_avg: u64,
        udf_executions: u64,
        backend: Arc<StateBackend>,
        id: String,
    }

    impl Task {
        pub fn new(id: String, udf: Module, backend: Arc<StateBackend>) -> Task {
            Task {
                ctx: ComponentContext::new(),
                report_timer: None,
                manager_port: ProvidedPort::new(),
                udf,
                udf_avg: 0,
                udf_executions: 0,
                backend: Arc::clone(&backend),
                id,
            }
        }

        fn stop_report(&mut self) {
            if let Some(timer) = self.report_timer.clone() {
                self.cancel_timer(timer);
                self.report_timer = None;
            }
        }

        fn run_udf(&mut self) {
            // Example for now.
            // Assuming the return type is i32
            let input: i32 = 10;
            let ref mut ctx = WeldContext::new(&self.udf.conf()).unwrap();
            let result: ModuleRun<i32> = self.udf.run(&input, ctx).unwrap();
            let ns = result.1;
            self.update_avg(ns);
        }

        fn update_avg(&mut self, ns: u64) {
            if self.udf_executions == 0 {
                self.udf_avg = ns;
            } else {
                let ema: i32 = ((ns as f32 - self.udf_avg as f32)
                    * (2.0 / (self.udf_executions + 1) as f32))
                    as i32
                    + self.udf_avg as i32;
                self.udf_avg = ema as u64;
            }
            self.udf_executions += 1;
        }
    }

    impl Provide<ControlPort> for Task {
        fn handle(&mut self, event: ControlEvent) -> () {
            match event {
                ControlEvent::Start => {
                    let timeout = Duration::from_millis(250);
                    let timer = self.schedule_periodic(timeout, timeout, |self_c, _| {
                        self_c.actor_ref().tell(Box::new(MetricReport {}), self_c);
                    });

                    self.report_timer = Some(timer);
                }
                ControlEvent::Stop => self.stop_report(),
                ControlEvent::Kill => self.stop_report(),
            }
        }
    }

    impl Actor for Task {
        fn receive_local(&mut self, _sender: ActorRef, msg: Box<Any>) {
            if let Ok(_report) = msg.downcast::<MetricReport>() {
                self.manager_port.trigger(Metric {
                    task_id: self.id.clone(),
                    task_avg: self.udf_avg,
                });
            }
            // Snapshot State
            //if let Ok(_) = self.backend.snapshot() {
            //
            //}
        }
        fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
            // Process Element
            // Process Watermark
            // self.run_udf()
        }
    }

    impl Provide<MetricPort> for Task {
        fn handle(&mut self, event: Metric) -> () {
            // TODO: Remove??
        }
    }

    #[test]
    fn task_test() {
        let self_socket_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), 0);
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());

        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let id = String::from("addition");
        let code = String::from("|x:i32| 40 + x");
        let priority = 0;
        let module = Module::new(id, code, priority, None).unwrap();
        let db = in_memory::InMemory::create("test_storage");

        let (task, _) =
            system.create_and_register(move || Task::new("f1".to_string(), module, Arc::new(db)));

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
        let task_port = task.on_definition(|c| {
            c.manager_port.connect(tm_port);
            c.manager_port.share();
        });

        system.start(&task_manager);
        system.start(&task);
        std::thread::sleep(Duration::from_millis(500));
        system.shutdown().expect("fail");
    }
}
