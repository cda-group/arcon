// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Nexmark queries

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

use anyhow::Result;
use clap::{App, AppSettings, Arg, SubCommand};
use experiments::nexmark::config::*;
use arcon::prelude::{ArconPipeline, ArconConf};
use std::fs::metadata;

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

    let arcon_config_path = Arg::with_name("a")
        .takes_value(true)
        .long("Arcon config")
        .short("a")
        .help("Path to Arcon Config");

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
                .arg(&arcon_config_path)
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
            let nexmark_config_path = arg_matches
                .value_of("c")
                .expect("Should not happen as there is a default")
                .parse::<String>()
                .unwrap();

            let arcon_path_opt = arg_matches.value_of("a");

            let arcon_config_path = {
                if let Some(p) = arcon_path_opt {
                    Some(p.to_string())
                } else {
                    None
                }
            };

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
                &nexmark_config_path,
                arcon_config_path,
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
    nexmark_config_path: &str,
    arcon_config_path: Option<String>,
    _batch_size: u64,
    _log_freq: u64,
    _dedicated: bool,
    _pinned: bool,
    _log_throughput: bool,
) -> Result<()> {

    let nexmark_config_file: String = {
        let md = metadata(&nexmark_config_path)?;
        if md.is_file() {
            nexmark_config_path.to_string()
        } else {
            nexmark_config_path.to_owned() + "/" + DEFAULT_NEXMARK_CONFIG
        }
    };

    // Load Base conf
    let mut nexmark_config = NEXMarkConfig::load(&nexmark_config_file)?;
    // Finish the conf
    NEXMarkConfig::finish(&mut nexmark_config);
    info!("{:?}\n", nexmark_config);

    // Set up ArconPipeline
    let pipeline = {
        if let Some(path) = arcon_config_path {
            let conf = ArconConf::from_file(&path).unwrap();
            ArconPipeline::with_conf(conf)
        } else {
            ArconPipeline::new()
        }
    };

    info!("{:?}\n", pipeline.arcon_conf());
    // Setup pipeline...
    match nexmark_config.query {
        NEXMarkQuery::CurrencyConversion => {
            info!("Setting up CurrencyConversion query");
        }
    }

    Ok(())
}
