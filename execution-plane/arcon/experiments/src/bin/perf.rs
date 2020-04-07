// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// A simple pipeline to profile arcon.
// Can be used to identify performance regressions..

#[macro_use]
extern crate clap;

use arcon::prelude::*;
use clap::{App, AppSettings, Arg, SubCommand};
use experiments::{
    get_items, square_root_newton,
    throughput_sink::{Run, ThroughputSink},
    EnrichedItem, Item,
};
use std::usize;

fn main() {
    let kompact_threads_arg = Arg::with_name("t")
        .required(false)
        .default_value("4")
        .takes_value(true)
        .long("Threads for the KompactSystem")
        .short("t")
        .help("Threads for the KompactSystem");

    let batch_size_arg = Arg::with_name("b")
        .required(false)
        .default_value("1024")
        .takes_value(true)
        .long("Batch size for ChannelStrategy")
        .short("b")
        .help("Batch size for ChannelStrategy");

    let collection_size_arg = Arg::with_name("z")
        .required(false)
        .default_value("10000000")
        .takes_value(true)
        .long("Amount of items for the collection source")
        .short("z")
        .help("Amount of items for the collection source");

    let kompact_throughput_arg = Arg::with_name("k")
        .required(false)
        .default_value("50")
        .takes_value(true)
        .long("Kompact cfg throughput")
        .short("k")
        .help("kompact cfg throughput");

    let log_frequency_arg = Arg::with_name("f")
        .required(false)
        .default_value("100000")
        .takes_value(true)
        .long("How often we log throughput")
        .short("f")
        .help("throughput log freq");

    let scaling_factor_arg = Arg::with_name("s")
        .required(false)
        .default_value("1")
        .takes_value(true)
        .long("workload scaling")
        .short("s")
        .help("workload scaling");

    let matches = App::new("Perf")
        .setting(AppSettings::ColoredHelp)
        .version(crate_version!())
        .setting(AppSettings::SubcommandRequired)
        .arg(
            Arg::with_name("d")
                .help("dedicated mode")
                .long("dedicated")
                .short("d"),
        )
        .arg(
            Arg::with_name("p")
                .help("dedicated-pinned mode")
                .long("dedicated-pinned")
                .short("p"),
        )
        .arg(
            Arg::with_name("log")
                .help("log-throughput")
                .long("log pipeline throughput")
                .short("log"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .setting(AppSettings::ColoredHelp)
                .arg(&kompact_throughput_arg)
                .arg(&kompact_threads_arg)
                .arg(&batch_size_arg)
                .arg(&log_frequency_arg)
                .arg(&collection_size_arg)
                .arg(&scaling_factor_arg)
                .about("Run Perf"),
        )
        .get_matches_from(fetch_args());

    let dedicated: bool = matches.is_present("d");
    let pinned: bool = matches.is_present("p");
    let log_throughput: bool = matches.is_present("log");

    match matches.subcommand() {
        ("run", Some(arg_matches)) => {
            let log_freq = arg_matches
                .value_of("f")
                .expect("Should not happen as there is a default")
                .parse::<u64>()
                .unwrap();

            let batch_size = arg_matches
                .value_of("b")
                .expect("Should not happen as there is a default")
                .parse::<u64>()
                .unwrap();

            let collection_size = arg_matches
                .value_of("z")
                .expect("Should not happen as there is a default")
                .parse::<usize>()
                .unwrap();

            let kompact_throughput = arg_matches
                .value_of("k")
                .expect("Should not happen as there is a default")
                .parse::<u64>()
                .unwrap();

            let scaling_factor = arg_matches
                .value_of("s")
                .expect("Should not happen as there is a default")
                .parse::<u64>()
                .unwrap();
            let kompact_threads = arg_matches
                .value_of("t")
                .expect("Should not happen as there is a default")
                .parse::<usize>()
                .unwrap();

            exec(
                scaling_factor,
                collection_size,
                batch_size,
                kompact_threads,
                log_freq,
                kompact_throughput,
                dedicated,
                pinned,
                log_throughput,
            );
        }
        _ => {
            panic!("Wrong arg");
        }
    }
}

fn fetch_args() -> Vec<String> {
    std::env::args().collect()
}

// CollectionSource -> Map Node -> ThroughputSink
fn exec(
    scaling_factor: u64,
    collection_size: usize,
    batch_size: u64,
    kompact_threads: usize,
    log_freq: u64,
    kompact_throughput: u64,
    dedicated: bool,
    pinned: bool,
    log_throughput: bool,
) {
    let core_ids = arcon::prelude::get_core_ids().unwrap();
    // So we don't pin on cores that the Kompact workers also pinned to.
    let mut core_counter: usize = kompact_threads;
    let timeout = std::time::Duration::from_millis(500);

    let mut cfg = KompactConfig::default();
    cfg.threads(kompact_threads);
    if !dedicated {
        cfg.throughput(kompact_throughput as usize);
        cfg.msg_priority(1.0);
    }

    let system = cfg.build().expect("KompactSystem");

    let total_items = collection_size.next_power_of_two();

    let sink = system.create(move || {
        ThroughputSink::<experiments::EnrichedItem>::new(
            log_freq,
            log_throughput,
            total_items as u64,
        )
    });
    let sink_port = sink.on_definition(|cd| cd.sink_port.share());

    system
        .start_notify(&sink)
        .wait_timeout(timeout)
        .expect("sink never started!");

    let sink_ref: ActorRefStrong<ArconMessage<EnrichedItem>> = sink.actor_ref().hold().expect("no");
    let sink_channel = Channel::Local(sink_ref);

    // Map comp...
    #[inline(always)]
    fn map_fn(item: Item) -> EnrichedItem {
        // Workload function that we can adjust with the scaling factor
        let root = square_root_newton(item.number, item.scaling_factor as usize);

        EnrichedItem {
            id: item.id,
            root: Some(root),
        }
    }

    let channel_strategy = ChannelStrategy::Forward(Forward::with_batch_size(
        sink_channel,
        NodeID::new(1),
        batch_size as usize,
    ));

    let node = Node::<Item, EnrichedItem>::new(
        1.into(),
        vec![2.into()],
        channel_strategy,
        Box::new(Map::new(&map_fn)),
        Box::new(InMemory::new("perf").unwrap()),
        ".".into(),
    );

    let map_node = if dedicated {
        if pinned {
            assert!(core_counter < core_ids.len());
            core_counter += 1;
            println!(
                "Starting Map node using pinned component on core id {}",
                core_counter - 1
            );
            system.create_dedicated_pinned(move || node, core_ids[core_counter - 1])
        } else {
            system.create_dedicated(move || node)
        }
    } else {
        system.create(move || node)
    };

    system
        .start_notify(&map_node)
        .wait_timeout(timeout)
        .expect("map_node never started!");

    // Set up Source

    // Just an identity function
    fn mapper(item: Item) -> Item {
        item
    }
    let source_op = Box::new(Map::<Item, Item>::new(&mapper));
    let watermark_interval = batch_size * 4;

    // Set up channel for source to Map node
    let node_ref: ActorRefStrong<ArconMessage<Item>> = map_node.actor_ref().hold().expect("no");
    let node_channel = Channel::Local(node_ref);
    let channel_strategy = ChannelStrategy::Forward(Forward::with_batch_size(
        node_channel,
        NodeID::new(2),
        batch_size as usize,
    ));

    let source_context: SourceContext<Item, Item> = SourceContext::new(
        watermark_interval,
        None, // no timestamp extractor
        channel_strategy,
        source_op,
        Box::new(InMemory::new("test").unwrap()),
    );

    // Collection for source
    let items = get_items(scaling_factor, total_items as usize);
    println!("Finished generating items...");

    // Set up CollectionSource component
    let collection_source: CollectionSource<Item, Item> =
        CollectionSource::new(items, source_context);

    let source = {
        if pinned {
            assert!(core_counter < core_ids.len());
            core_counter += 1;
            println!(
                "Starting source using pinned component on core id {}",
                core_counter - 1
            );
            system.create_dedicated_pinned(move || collection_source, core_ids[core_counter - 1])
        } else {
            system.create_dedicated(move || collection_source)
        }
    };

    system
        .start_notify(&source)
        .wait_timeout(timeout)
        .expect("source never started!");

    // Set up start time at Sink
    let (promise, future) = kpromise();
    system.trigger_r(Run::new(promise), &sink_port);

    // wait for sink to return completion msg.
    let res = future.wait();
    println!("Execution took {:?} milliseconds", res.as_millis());

    let _ = system.shutdown();
}
