// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon::prelude::*;
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::time::Duration;

mod common;
use common::*;

const NUM_BATCHES: usize = 1000;

fn arcon_flight_serde(c: &mut Criterion) {
    let group = c.benchmark_group("arcon_flight_serde");
    // Something is broken in the serialisation there,
    // but if we are anyway changeing it, no point in fixing this now.
    //group.bench_function("Arcon Reliable Flight", reliable_serde);
    //group.bench_function("Arcon Unsafe Flight", unsafe_serde);
    group.finish()
}

fn setup_system() -> (KompactSystem, KompactSystem) {
    let system = || {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        cfg.build().expect("KompactSystem")
    };
    (system(), system())
}

pub fn reliable_serde(b: &mut Bencher) {
    arcon_flight_bench(b, FlightSerde::Reliable);
}

pub fn unsafe_serde(b: &mut Bencher) {
    arcon_flight_bench(b, FlightSerde::Unsafe);
}

pub fn arcon_flight_bench(b: &mut Bencher, serde: FlightSerde) {
    let pipeline = ArconPipeline::new();
    let pool_info = pipeline.get_pool_info();

    let batch_size = pipeline.arcon_conf().channel_batch_size;
    let total_msgs = batch_size * NUM_BATCHES;

    let (local, remote) = setup_system();
    let timeout = Duration::from_millis(150);
    let comp = remote.create(move || NodeReceiver::<i32>::new());
    remote
        .start_notify(&comp)
        .wait_timeout(timeout)
        .expect("comp never started");

    let comp_id = format!("remote_comp");
    let reg = remote.register_by_alias(&comp, comp_id.clone());
    reg.wait_expect(
        Duration::from_millis(1000),
        "Failed to register alias for Component",
    );
    let remote_path = ActorPath::Named(NamedPath::with_system(remote.system_path(), vec![
        comp_id.into()
    ]));

    let channel = Channel::Remote(remote_path, serde, local.dispatcher_ref().into());
    let mut channel_strategy: ChannelStrategy<i32> =
        ChannelStrategy::Forward(Forward::new(channel, 1.into(), pool_info));

    let experiment_port = comp.on_definition(|cd| cd.experiment_port.share());
    let mut events: Vec<Vec<ArconEvent<i32>>> = Vec::with_capacity(NUM_BATCHES);
    for _ in 0..NUM_BATCHES {
        let mut batch = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let event = ArconEvent::Element(ArconElement::new(i as i32));
            batch.push(event);
        }
        events.push(batch);
    }

    b.iter(|| {
        let (promise, future) = kpromise();
        remote.trigger_r(
            crate::Run::new(total_msgs as u64, promise),
            &experiment_port,
        );

        for batch in events.clone() {
            for event in batch {
                channel_strategy.add(event);
            }
        }

        let res = future.wait();
        res
    });

    drop(experiment_port);
    drop(comp);
    local.shutdown().expect("System did not shutdown!");
    remote.shutdown().expect("System did not shutdown!");
}
fn custom_criterion() -> Criterion {
    Criterion::default().sample_size(10)
}

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets = arcon_flight_serde,
}
criterion_main!(benches);
