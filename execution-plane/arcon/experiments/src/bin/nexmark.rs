// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Nexmark queries

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

use anyhow::Result;
use clap::{App, AppSettings, Arg, SubCommand};
use std::fs::metadata;
use experiments::nexmark::config::*;

const DEFAULT_NEXMARK_CONFIG: &str = "nexmark_config.toml";

fn main() {
    pretty_env_logger::init();

    let nexmark_config_arg = Arg::with_name("c")
        .required(true)
        .default_value(".")
        .takes_value(true)
        .long("Nexmark config")
        .short("c")
        .help("Path to Nexmark Config");

    let batch_size_arg = Arg::with_name("b")
        .required(false)
        .default_value("1024")
        .takes_value(true)
        .long("Batch size for ChannelStrategy")
        .short("b")
        .help("Batch size for ChannelStrategy");

    let log_frequency_arg = Arg::with_name("f")
        .required(false)
        .default_value("100000")
        .takes_value(true)
        .long("How often we log throughput")
        .short("f")
        .help("throughput log freq");

    let matches = App::new("Arcon Nexmark Queries")
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
                .arg(&nexmark_config_arg)
                .arg(&batch_size_arg)
                .arg(&log_frequency_arg)
                .about("Run Nexmark Query"),
        )
        .get_matches_from(fetch_args());

    let dedicated: bool = matches.is_present("d");
    let pinned: bool = matches.is_present("p");
    let log_throughput: bool = matches.is_present("log");

    match matches.subcommand() {
        ("run", Some(arg_matches)) => {
            let config_path = arg_matches
                .value_of("c")
                .expect("Should not happen as there is a default")
                .parse::<String>()
                .unwrap();

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


            if let Err(err) = run(
                &config_path,
                batch_size,
                log_freq,
                dedicated,
                pinned,
                log_throughput,
            ) {
                error!("{}", err.to_string());
            }
        }
        _ => {
            panic!("Wrong arg");
        }
    }
}

fn fetch_args() -> Vec<String> {
    std::env::args().collect()
}

fn run(
    config_path: &str,
    _batch_size: u64,
    _log_freq: u64,
    _dedicated: bool,
    _pinned: bool,
    _log_throughput: bool,
) -> Result<()> {

    // ArconPipeline::with_conf(..)

    let config_file: String = {
        let md = metadata(&config_path)?;
        if md.is_file() {
            config_path.to_string()
        } else {
            config_path.to_owned() + "/" + DEFAULT_NEXMARK_CONFIG
        }
    };

    // Load Base conf
    let mut nexmark_config = NEXMarkConfig::load(&config_file)?;
    // Finish the conf
    NEXMarkConfig::finish(&mut nexmark_config);
    info!("NEXMark Config {:?}\n", nexmark_config);

    // Setup pipeline...
    match nexmark_config.query {
        NEXMarkQuery::CurrencyConversion => {
            info!("Setting up CurrencyConversion query");
        }
    }

    Ok(())
}
