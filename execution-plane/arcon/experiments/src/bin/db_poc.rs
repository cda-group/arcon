// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

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

    let log_frequency_arg = Arg::with_name("f")
        .required(false)
        .default_value("100000")
        .takes_value(true)
        .long("How often we log throughput")
        .short("f")
        .help("throughput log freq");

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

    let matches = App::new("DbPOC")
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
                .arg(&kompact_threads_arg)
                .arg(&batch_size_arg)
                .arg(&collection_size_arg)
                .arg(&log_frequency_arg)
                .about("Run DB_POC"),
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

            let kompact_threads = arg_matches
                .value_of("t")
                .expect("Should not happen as there is a default")
                .parse::<usize>()
                .unwrap();

            exec(
                collection_size,
                batch_size,
                kompact_threads,
                dedicated,
                pinned,
                log_freq,
                true,
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

fn exec(
    collection_size: usize,
    batch_size: u64,
    kompact_threads: usize,
    dedicated: bool,
    pinned: bool,
    log_freq: u64,
    log_throughput: bool,
) {
    let core_ids = arcon::prelude::get_core_ids().unwrap();
    // So we don't pin on cores that the Kompact workers also pinned to.
    let mut core_counter: usize = kompact_threads;
    let timeout = std::time::Duration::from_millis(500);

    let mut cfg = KompactConfig::default();
    cfg.threads(kompact_threads);

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


    // Source -> Keyed Map -> Sink?
}
