use fnv::FnvHashMap;
use kompact::*;

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
    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
        unimplemented!();
    }
}
