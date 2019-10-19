// Benchmarks for Arcon Nodes
//
// NOTE: Most of the code is shamelessly stolen from:
// https://github.com/kompics/kompact/blob/master/experiments/dynamic-benches/src/network_latency.rs

use arcon::prelude::*;
use criterion::{criterion_group, Bencher, Criterion};
use std::time::{Duration, Instant};

const NODE_MSGS: usize = 100000;

fn arcon_node_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_node_latency");
    group.bench_function("Forward Map Test", node_latency_forward);

    group.finish()
}

fn setup_system(name: &'static str, throughput: usize) -> KompactSystem {
    let mut cfg = KompactConfig::new();
    cfg.label(name.to_string());
    cfg.throughput(throughput);
    cfg.msg_priority(1.0);
    cfg.build().expect("KompactSystem")
}

pub fn node_latency_forward(b: &mut Bencher) {
    node_run(b, NODE_MSGS);
}

pub fn node_run(b: &mut Bencher, messages: usize) {
    let sys = setup_system("node_system", messages);

    let timeout = Duration::from_millis(500);

    let node_receiver = sys.create(move || NodeReceiver::new());

    let actor_ref: ActorRef<ArconMessage<i32>> = node_receiver.actor_ref();
    let channel = Channel::Local(actor_ref);
    let channel_strategy: Box<dyn ChannelStrategy<i32>> = Box::new(Forward::new(channel));

    let code = String::from("|x: i32| x + 10");
    let module = std::sync::Arc::new(Module::new(code).unwrap());
    let node_comp = Node::<i32, i32>::new(
        0.into(),
        vec![1.into()],
        channel_strategy,
        Box::new(Map::new(module)),
    );

    let node = sys.create(|| node_comp);

    // Bit hacky, but since we depend on the receiver to create the node itself.
    let _ = node_receiver.on_definition(|cd| cd.set_node(node.actor_ref()));

    let experiment_port = node_receiver.on_definition(|cd| cd.experiment_port.share());

    sys.start_notify(&node_receiver)
        .wait_timeout(timeout)
        .expect("node_receiver never started!");

    sys.start_notify(&node)
        .wait_timeout(timeout)
        .expect("node never started!");

    b.iter_custom(|num_iterations| {
        let (promise, future) = kpromise();
        sys.trigger_r(Run::new(num_iterations, promise), &experiment_port);
        let res = future.wait();
        res
    });

    drop(experiment_port);
    drop(node_receiver);
    drop(node);
    sys.shutdown().expect("System did not shutdown!");
}

#[derive(Debug)]
pub struct Run {
    num_iterations: u64,
    promise: KPromise<Duration>,
}
impl Run {
    pub fn new(num_iterations: u64, promise: KPromise<Duration>) -> Run {
        Run {
            num_iterations,
            promise,
        }
    }
}
impl Clone for Run {
    fn clone(&self) -> Self {
        unimplemented!("Shouldn't be invoked in this experiment!");
    }
}

pub struct ExperimentPort;
impl Port for ExperimentPort {
    type Indication = ();
    type Request = Run;
}

#[derive(ComponentDefinition)]
pub struct NodeReceiver {
    ctx: ComponentContext<Self>,
    pub experiment_port: ProvidedPort<ExperimentPort, Self>,
    done: Option<KPromise<Duration>>,
    node: Option<ActorRef<ArconMessage<i32>>>,
    remaining_send: u64,
    remaining_recv: u64,
    start: Instant,
}
impl NodeReceiver {
    pub fn new() -> NodeReceiver {
        NodeReceiver {
            ctx: ComponentContext::new(),
            experiment_port: ProvidedPort::new(),
            done: None,
            node: None,
            remaining_send: 0,
            remaining_recv: 0,
            start: Instant::now(),
        }
    }

    pub fn set_node(&mut self, node: ActorRef<ArconMessage<i32>>) {
        self.node = Some(node);
    }
}

impl Provide<ControlPort> for NodeReceiver {
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl Actor for NodeReceiver {
    type Message = ArconMessage<i32>;

    fn receive_local(&mut self, _msg: Self::Message) -> () {
        self.remaining_recv -= 1u64;
        if self.remaining_recv <= 0u64 {
            let time = self.start.elapsed();
            let promise = self.done.take().expect("No promise to reply to?");
            promise.fulfill(time).expect("Promise was dropped");
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> () {
        unimplemented!("Not being tested!");
    }
}

impl Provide<ExperimentPort> for NodeReceiver {
    fn handle(&mut self, event: Run) -> () {
        self.remaining_send = event.num_iterations;
        self.remaining_recv = event.num_iterations;
        self.done = Some(event.promise);
        self.start = Instant::now();

        let sender_id: NodeID = 1.into();

        let node = self.node.as_ref().expect("ActorRef not set");

        while self.remaining_send > 0u64 {
            self.remaining_send -= 1u64;
            let msg = ArconMessage::element(100 as i32, None, sender_id);
            node.tell(msg);
        }
    }
}

criterion_group!(benches, arcon_node_latency);
