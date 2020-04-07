// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Benchmarks for Arcon Nodes
//
// NOTE: Most of the code is shamelessly stolen from:
// https://github.com/kompics/kompact/blob/master/experiments/dynamic-benches/src/network_latency.rs

use arcon::prelude::*;
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::time::Duration;

mod common;
use common::*;

const NODE_MSGS: usize = 100000;
const BATCH_SIZE: usize = 1000;
const KOMPACT_THROUGHPUT: usize = 25;

fn arcon_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_node");
    group.bench_function("Forward Test", node_forward);

    group.finish()
}

fn setup_system(name: &'static str, throughput: usize) -> KompactSystem {
    let mut cfg = KompactConfig::new();
    cfg.label(name.to_string());
    cfg.throughput(throughput);
    cfg.msg_priority(1.0);
    cfg.build().expect("KompactSystem")
}

pub fn node_forward(b: &mut Bencher) {
    node_forward_bench(b, KOMPACT_THROUGHPUT);
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
        Box::new(InMemory::new("bench").unwrap()),
        ".".into(),
    );

    let node = sys.create(|| node_comp);

    let experiment_port = node_receiver.on_definition(|cd| cd.experiment_port.share());

    sys.start_notify(&node_receiver)
        .wait_timeout(timeout)
        .expect("node_receiver never started!");

    sys.start_notify(&node)
        .wait_timeout(timeout)
        .expect("node never started!");

    let mut buffer: Vec<ArconEventWrapper<i32>> = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        buffer.push(ArconEvent::Element(ArconElement::new(i as i32)).into());
    }

    b.iter(|| {
        let (promise, future) = kpromise();
        sys.trigger_r(Run::new(NODE_MSGS as u64, promise), &experiment_port);

        let batches = NODE_MSGS / BATCH_SIZE;
        for _batch in 0..batches {
            let msg = ArconMessage {
                events: buffer.clone(),
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

fn custom_criterion() -> Criterion {
    Criterion::default().sample_size(10)
}

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets = arcon_node,
}

criterion_main!(benches);
