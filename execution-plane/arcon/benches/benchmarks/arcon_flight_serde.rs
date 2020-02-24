// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon::prelude::*;
use criterion::{criterion_group, Bencher, Criterion};

const TOTAL_MSGS: usize = 1024000;
const BATCH_SIZE: usize = 1024;

fn arcon_flight_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_flight_serde");
    group.bench_function("Arcon Reliable Flight", reliable_serde);
    group.bench_function("Arcon Unsafe Flight", unsafe_serde);
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
    let (local, remote) = setup_system();
    let timeout = std::time::Duration::from_millis(150);
    let comp = remote.create(move || super::NodeReceiver::<i32>::new());
    remote
        .start_notify(&comp)
        .wait_timeout(timeout)
        .expect("comp never started");

    let comp_id = format!("remote_comp");
    let _ = remote.register_by_alias(&comp, comp_id.clone());
    let remote_path = ActorPath::Named(NamedPath::with_system(remote.system_path(), vec![
        comp_id.into()
    ]));

    let channel = Channel::Remote(remote_path, serde, local.dispatcher_ref().into());
    let mut channel_strategy: ChannelStrategy<i32> =
        ChannelStrategy::Forward(Forward::with_batch_size(channel, 1.into(), BATCH_SIZE));

    let experiment_port = comp.on_definition(|cd| cd.experiment_port.share());
    let mut events: Vec<Vec<ArconEvent<i32>>> = Vec::with_capacity(BATCH_SIZE);
    for _ in 0..BATCH_SIZE {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        for i in 0..BATCH_SIZE {
            let event = ArconEvent::Element(ArconElement::new(i as i32));
            batch.push(event);
        }
        events.push(batch);
    }

    b.iter(|| {
        let (promise, future) = kpromise();
        remote.trigger_r(
            super::Run::new(TOTAL_MSGS as u64, promise),
            &experiment_port,
        );

        for batch in events.clone() {
            for event in batch {
                channel_strategy.add(event);
            }
        }
        channel_strategy.flush();
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
