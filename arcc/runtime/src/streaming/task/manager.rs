use akka_api::messages::KompactAkkaMsg_oneof_payload::*;
use akka_api::messages::*;
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

/// The `Manager` handles metrics from
/// each StreamTask. It is supposed to send
/// off the metrics to Arcon's operational plane
/// ,but it may also on instruction alter the
/// current running StreamTasks
#[derive(ComponentDefinition)]
pub struct Manager {
    ctx: ComponentContext<Manager>,
    metric_port: ProvidedPort<MetricPort, Manager>,
    task_metrics: FnvHashMap<TaskId, TaskAvg>,
}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            ctx: ComponentContext::new(),
            metric_port: ProvidedPort::new(),
            task_metrics: FnvHashMap::default(),
        }
    }

    fn handle_msg(&mut self, envelope: KompactAkkaEnvelope) -> crate::error::ArconResult<()> {
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

impl Provide<ControlPort> for Manager {
    fn handle(&mut self, event: ControlEvent) -> () {
        if let ControlEvent::Start = event {
            debug!(self.ctx.log(), "Starting Task Manager");
        }
    }
}

impl Provide<MetricPort> for Manager {
    fn handle(&mut self, metric: Metric) {
        debug!(self.ctx.log(), "Got metric {:?}", metric);
        self.task_metrics.insert(metric.task_id, metric.task_avg);
    }
}

impl Actor for Manager {
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
