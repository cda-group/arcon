// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Nexmark queries

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

use anyhow::Result;
use arcon::prelude::{ArconConf, ArconPipeline};
use clap::{App, AppSettings, Arg, SubCommand};
use experiments::nexmark::{config::*, queries};
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

    let tui_arg = Arg::with_name("t").help("toggle tui mode on").short("t");
    let debug_arg = Arg::with_name("d").help("toggle debug mode on").short("d");

    let query_arg = Arg::with_name("q")
        .required(false)
        .default_value("1")
        .takes_value(true)
        .long("NEXMark query")
        .short("q")
        .help("NEXMark query");

    let matches = App::new("Arcon Nexmark Queries")
        .setting(AppSettings::ColoredHelp)
        .version(crate_version!())
        .setting(AppSettings::SubcommandRequired)
        .subcommand(
            SubCommand::with_name("run")
                .setting(AppSettings::ColoredHelp)
                .arg(&query_arg)
                .arg(&nexmark_config_arg)
                .arg(&arcon_config_path)
                .arg(&tui_arg)
                .arg(&debug_arg)
                .about("Run Nexmark Query"),
        )
        .get_matches_from(fetch_args());

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

            let mut tui = arg_matches.is_present("t");
            let mut debug_mode = arg_matches.is_present("d");
            if debug_mode {
                // If debug mode enabled, disable tui...
                tui = false;
            }
            if tui {
                debug_mode = false;
            }

            let query = arg_matches
                .value_of("q")
                .expect("Should not happen as there is a default")
                .parse::<u8>()
                .unwrap();

            if let Err(err) = run(
                query,
                &nexmark_config_path,
                arcon_config_path,
                tui,
                debug_mode,
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
    nexmark_query: u8,
    nexmark_config_path: &str,
    arcon_config_path: Option<String>,
    tui: bool,
    debug_mode: bool,
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
    let mut nexmark_config = {
        if let Ok(conf) = NEXMarkConfig::load(&nexmark_config_file) {
            conf
        } else {
            // If loading from file failed, just load a default one...
            let mut conf = NEXMarkConfig::default();
            let q: NEXMarkQuery = unsafe { ::std::mem::transmute(nexmark_query) };
            conf.query = q;
            conf
        }
    };
    // Finish the conf
    NEXMarkConfig::finish(&mut nexmark_config);
    info!("{:?}\n", nexmark_config);

    // Set up ArconPipeline
    let mut pipeline = {
        if let Some(path) = arcon_config_path {
            let conf = ArconConf::from_file(&path).unwrap();
            ArconPipeline::with_conf(conf)
        } else {
            ArconPipeline::new()
        }
    };

    info!("{:?}\n", pipeline.arcon_conf());

    let pipeline_timer = match nexmark_config.query {
        NEXMarkQuery::CurrencyConversion => {
            info!("Running CurrencyConversion query");
            queries::q1::q1(debug_mode, nexmark_config, &mut pipeline)
        }
        NEXMarkQuery::LocalItemSuggestion => {
            info!("Running LocalItemSuggestion query");
            queries::q3::q3(debug_mode, nexmark_config, &mut pipeline)
        }
    };

    if tui {
        pipeline.tui();
    } else {
        if let Some(timer_future) = pipeline_timer {
            // wait for sink to return completion msg.
            let res = timer_future.wait();
            println!("Execution took {:?} milliseconds", res.as_millis());
        } else {
            pipeline.await_termination();
        }
    }

    Ok(())
}
