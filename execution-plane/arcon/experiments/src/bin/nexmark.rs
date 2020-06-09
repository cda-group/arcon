// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Nexmark queries
#[macro_use]
extern crate log;
#[macro_use]
extern crate prettytable;

use anyhow::Result;
use arcon::prelude::{ArconConf, ArconPipeline, *};
use experiments::nexmark::{config::*, queries, queries::Query};
use queries::{q1::QueryOne, q3::QueryThree};
use std::{
    fs::metadata,
    path::{Path, PathBuf},
};
use structopt::{clap::arg_enum, StructOpt};

const DEFAULT_NEXMARK_CONFIG: &str = "nexmark_config.toml";

#[derive(StructOpt)]
struct Opts {
    /// Path to Nexmark Config
    #[structopt(short = "c", long, default_value = ".")]
    nexmark_config: PathBuf,
    /// Path to Arcon Config
    #[structopt(short = "a", long)]
    arcon_config: Option<PathBuf>,
    /// Toggle tui mode on
    #[structopt(short = "t", long)]
    tui: bool,
    /// Toggle debug mode on
    #[structopt(short = "d", long)]
    debug: bool,
    /// NEXMark query
    #[structopt(short = "q", long)]
    query: Option<u8>,
    /// State backend type
    #[structopt(
        long,
        possible_values = StateBackendType::STR_VARIANTS,
        case_insensitive = true,
        default_value = "InMemory"
    )]
    state_backend_type: state::BackendType,
}

fn main() {
    pretty_env_logger::init();

    let Opts {
        nexmark_config,
        arcon_config,
        mut tui,
        mut debug,
        query,
        state_backend_type,
    } = Opts::from_args();

    if debug {
        // If debug mode enabled, disable tui...
        tui = false;
    }
    if tui {
        debug = false;
    }

    if let Err(err) = run(
        query,
        &nexmark_config,
        arcon_config,
        tui,
        debug,
        state_backend_type,
    ) {
        error!("{}", err.to_string());
    }
}

fn run(
    nexmark_query: Option<u8>,
    nexmark_config_path: &Path,
    arcon_config_path: Option<PathBuf>,
    tui: bool,
    debug_mode: bool,
    state_backend_type: StateBackendType,
) -> Result<()> {
    let nexmark_config_file: PathBuf = {
        let md = metadata(&nexmark_config_path)?;
        if md.is_file() {
            nexmark_config_path.to_owned()
        } else {
            let mut p = nexmark_config_path.to_owned();
            p.push(DEFAULT_NEXMARK_CONFIG);
            p
        }
    };
    // Load Base conf
    let mut nexmark_config = NEXMarkConfig::load(&nexmark_config_file).unwrap_or_else(|e| {
        warn!("Couldn't load config: {}", e);
        info!("Falling back to default");
        NEXMarkConfig::default()
    });
    if let Some(query) = nexmark_query {
        nexmark_config.query = NEXMarkQuery::new(query)?;
    }
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
            QueryOne::run(debug_mode, nexmark_config, &mut pipeline)
        }
        NEXMarkQuery::LocalItemSuggestion => {
            info!("Running LocalItemSuggestion query");
            QueryThree::run(debug_mode, nexmark_config, &mut pipeline)
        }
    };

    if tui {
        pipeline.tui();
    } else {
        if let Some(timer_future) = pipeline_timer {
            // wait for sink to return completion msg.
            let res = timer_future.wait();
            let execution_ms = res.timer.as_millis();
            let table = table!(["Runtime (ms)", "Events per sec"], [
                execution_ms.to_string(),
                res.events_per_sec.to_string()
            ]);
            table.printstd();
        } else {
            pipeline.await_termination();
        }
    }

    Ok(())
}
