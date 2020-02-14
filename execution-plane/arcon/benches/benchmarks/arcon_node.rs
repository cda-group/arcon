// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Benchmarks for Arcon Nodes
//
// NOTE: Most of the code is shamelessly stolen from:
// https://github.com/kompics/kompact/blob/master/experiments/dynamic-benches/src/network_latency.rs

use arcon::prelude::*;
use criterion::{criterion_group, Bencher, Criterion};
use std::time::{Duration, Instant};

const NODE_MSGS: usize = 100000;
const BATCH_SIZE: usize = 10000;

fn arcon_node_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_node_latency");
    group.bench_function("Forward Test", node_latency_forward);

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
    node_forward_bench(b, NODE_MSGS);
}

pub fn node_forward_bench(b: &mut Bencher, messages: usize) {
    let sys = setup_system("node_forward", messages);

    let timeout = Duration::from_millis(500);

    let node_receiver = sys.create(move || NodeReceiver::new());

    let actor_ref: ActorRefStrong<ArconMessage<i32>> = node_receiver
        .actor_ref()
        .hold()
        .expect("failed to fetch ref");
    let channel = Channel::Local(actor_ref);
    let channel_strategy = ChannelStrategy::Forward(Forward::with_batch_size(
        channel,
        NodeID::new(0),
        BATCH_SIZE,
    ));

    fn map_fn(x: i32) -> i32 {
        x + 10
    }

    let node_comp = Node::<i32, i32>::new(
        0.into(),
        vec![1.into()],
        channel_strategy,
        Box::new(Map::new(&map_fn)),
    );

    let node = sys.create(|| node_comp);

    // Bit hacky, but since we depend on the receiver to create the node itself.
    let _ = node_receiver.on_definition(|cd| cd.set_node(node.actor_ref().hold().expect("fail")));

    let experiment_port = node_receiver.on_definition(|cd| cd.experiment_port.share());

    sys.start_notify(&node_receiver)
        .wait_timeout(timeout)
        .expect("node_receiver never started!");

    sys.start_notify(&node)
        .wait_timeout(timeout)
        .expect("node never started!");

    b.iter(|| {
        let (promise, future) = kpromise();
        sys.trigger_r(Run::new(NODE_MSGS as u64, promise), &experiment_port);

        let batches = NODE_MSGS / BATCH_SIZE;

        for _batch in 0..batches {
            let mut buffer: Vec<ArconEvent<i32>> = Vec::with_capacity(BATCH_SIZE);
            for i in 0..BATCH_SIZE {
                buffer.push(ArconEvent::Element(ArconElement::new(i as i32)));
            }
            let msg = ArconMessage {
                events: buffer,
                sender: NodeID::new(1),
            };
            node.actor_ref().tell(msg);
        }
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
pub struct NodeReceiver<A: ArconType> {
    ctx: ComponentContext<Self>,
    pub experiment_port: ProvidedPort<ExperimentPort, Self>,
    done: Option<KPromise<Duration>>,
    node: Option<ActorRefStrong<ArconMessage<i32>>>,
    remaining_recv: u64,
    start: Instant,
}
impl<A: ArconType> NodeReceiver<A> {
    pub fn new() -> NodeReceiver<A> {
        NodeReceiver {
            ctx: ComponentContext::new(),
            experiment_port: ProvidedPort::new(),
            done: None,
            node: None,
            remaining_recv: 0,
            start: Instant::now(),
        }
    }

    pub fn set_node(&mut self, node: ActorRefStrong<ArconMessage<i32>>) {
        self.node = Some(node);
    }
}

impl<A: ArconType> Provide<ControlPort> for NodeReceiver<A> {
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl<A: ArconType> Actor for NodeReceiver<A> {
    type Message = ArconMessage<A>;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        self.remaining_recv -= msg.events.len() as u64;
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

impl<A: ArconType> Provide<ExperimentPort> for NodeReceiver<A> {
    fn handle(&mut self, event: Run) -> () {
        self.done = Some(event.promise);
        self.start = Instant::now();
        self.remaining_recv = event.num_iterations;
    }
}

criterion_group!(benches, arcon_node_latency);
