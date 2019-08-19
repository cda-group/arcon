#![feature(vec_remove_item)]
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate failure;
#[macro_use]
extern crate lazy_static;

use arcon_spec::*;
use clap::{App, AppSettings, Arg, SubCommand};
use ferris_says::say;
use std::fs::metadata;
use std::io::{stdout, BufWriter};
use std::str::FromStr;

mod env;
mod repl;
mod server;
mod util;

const DEFAULT_SERVER_PORT: i32 = 3000;
const DEFAULT_SERVER_HOST: &str = "127.0.0.1";
const DEFAULT_SPEC: &str = "spec.json";
const DEFAULT_BUILD_DIR: &str = "build";
const DEFAULT_LOG_DIR: &str = "/tmp";

lazy_static! {
    static ref TARGETS: Vec<String> = {
        let targets = util::target_list().expect("Failed to fetch target list");
        targets.split("\n").map(|x| x.to_string()).collect()
    };
    static ref RUSTC_VERSION: String =
        util::rustc_version().expect("Failed to fetch rustc version");
}

fn main() {
    pretty_env_logger::init();

    let default_port_str = DEFAULT_SERVER_PORT.to_string();

    let spec_arg = Arg::with_name("s")
        .required(true)
        .default_value(".")
        .takes_value(true)
        .long("spec")
        .short("s")
        .help("Path to Arcon specification");

    let port_arg = Arg::with_name("p")
        .default_value(&default_port_str)
        .takes_value(true)
        .long("Port number")
        .short("p")
        .help("Port for server");

    let host_arg = Arg::with_name("h")
        .default_value(DEFAULT_SERVER_HOST)
        .takes_value(true)
        .long("Host address")
        .short("h")
        .help("Address for server");

    let build_dir_arg = Arg::with_name("b")
        .required(false)
        .default_value("build/")
        .takes_value(true)
        .long("build-dir")
        .short("b")
        .help("Directory where arconc compiles into");

    let log_dir_arg = Arg::with_name("l")
        .required(false)
        .default_value(DEFAULT_LOG_DIR)
        .takes_value(true)
        .long("log-dir")
        .short("l")
        .help("Directory where logs are stored");

    let matches = App::new("Arcon Compiler")
        .setting(AppSettings::ColoredHelp)
        .author(crate_authors!("\n"))
        .version(crate_version!())
        .setting(AppSettings::SubcommandRequired)
        .arg(
            Arg::with_name("d")
                .help("Daemonize the process")
                .long("daemonize")
                .short("d"),
        )
        .subcommand(
            SubCommand::with_name("compile")
                .setting(AppSettings::ColoredHelp)
                .arg(&spec_arg)
                .arg(&build_dir_arg)
                .arg(&log_dir_arg)
                .about("Compile Arc Specification"),
        )
        .subcommand(
            SubCommand::with_name("server")
                .setting(AppSettings::ColoredHelp)
                .arg(&port_arg)
                .arg(&host_arg)
                .arg(&build_dir_arg)
                .arg(&log_dir_arg)
                .about("Launch Arcon Compiler in gRPC server mode"),
        )
        .subcommand(
            SubCommand::with_name("repl")
                .setting(AppSettings::ColoredHelp)
                .about("REPL playground"),
        )
        .subcommand(
            SubCommand::with_name("targets")
                .setting(AppSettings::ColoredHelp)
                .about("Prints available targets"),
        )
        .subcommand(
            SubCommand::with_name("rustc")
                .setting(AppSettings::ColoredHelp)
                .about("Prints rustc version"),
        )
        .get_matches_from(fetch_args());

    let daemonize: bool = matches.is_present("d");

    match matches.subcommand() {
        ("compile", Some(arg_matches)) => {
            let spec_path = arg_matches
                .value_of("s")
                .expect("Should not happen as there is a default");

            let build_dir: &str = arg_matches.value_of("b").unwrap_or(DEFAULT_BUILD_DIR);
            let log_dir: &str = arg_matches.value_of("l").unwrap_or(DEFAULT_LOG_DIR);

            if let Err(err) = compile(spec_path, build_dir, log_dir, daemonize) {
                error!("Error: {} ", err.to_string());
            }
        }
        ("server", Some(arg_matches)) => {
            let port: i32 = arg_matches
                .value_of("p")
                .and_then(|x| <i32 as FromStr>::from_str(x).ok())
                .unwrap_or(DEFAULT_SERVER_PORT);

            let host: &str = arg_matches.value_of("h").unwrap_or(DEFAULT_SERVER_HOST);
            let build_dir: &str = arg_matches.value_of("b").unwrap_or(DEFAULT_BUILD_DIR);
            let log_dir: &str = arg_matches.value_of("l").unwrap_or(DEFAULT_LOG_DIR);

            if let Err(err) = server(host, port, build_dir, log_dir, daemonize) {
                error!("Error: {} ", err.to_string());
            }
        }
        ("repl", Some(_)) => {
            if let Err(err) = repl() {
                error!("Error: {} ", err.to_string());
            }
        }
        ("targets", Some(_)) => {
            eprintln!("{:#?}", *TARGETS);
        }
        ("rustc", Some(_)) => {
            eprintln!("{}", *RUSTC_VERSION);
        }
        _ => eprintln!("Command did not match!"),
    }
}

fn fetch_args() -> Vec<String> {
    std::env::args().collect()
}

fn compile(
    spec_path: &str,
    build_dir: &str,
    log_dir: &str,
    daemonize: bool,
) -> Result<(), failure::Error> {
    let spec_file: String = {
        let md = metadata(&spec_path)?;
        if md.is_file() {
            spec_path.to_string()
        } else {
            (spec_path.to_owned() + "/" + DEFAULT_SPEC)
        }
    };

    let spec = ArconSpec::load(&spec_file)?;

    let mut env = env::CompilerEnv::load(build_dir.to_string())?;

    // Enter the build directory
    let path = std::path::Path::new(build_dir);
    std::env::set_current_dir(&path)?;

    env.add_project(spec.id.clone())?;
    env.create_workspace_member(&spec.id)?;
    env.generate(&spec)?;

    if daemonize {
        daemonize_arconc();
    } else {
        let bin = env.bin_path(&spec.id, &spec.mode)?;
        greeting_with_spec(&spec, &bin);
    }

    let logged = if daemonize {
        Some(log_dir.to_string())
    } else {
        None
    };

    util::cargo_build(&spec.id, logged, &spec.mode)?;

    Ok(())
}

fn server(
    host: &str,
    port: i32,
    build_dir: &str,
    log_dir: &str,
    daemonize: bool,
) -> Result<(), failure::Error> {
    let mut env = env::CompilerEnv::load(build_dir.to_string())?;

    if daemonize {
        env.add_log_dir(log_dir.to_string());
        daemonize_arconc();
    }

    let path = std::path::Path::new(build_dir);
    std::env::set_current_dir(&path)?;

    server::start_server(host, port, env);
    Ok(())
}

fn repl() -> Result<(), failure::Error> {
    error!("REPL is not implemented!");
    Ok(())
}

fn greeting_with_spec(spec: &ArconSpec, bin_path: &str) {
    let mode = match spec.mode {
        CompileMode::Debug => "debug",
        CompileMode::Release => "release",
    };

    let features_str = {
        if let Some(features) = spec.features.clone() {
            features.join(",")
        } else {
            "default".to_string()
        }
    };

    let msg = format!(
        "Wait while I compile {} for you!\n
         \n\nmode: {}\
         \nfeatures: {}\
         \npath: {}\n\n",
        spec.id, mode, features_str, bin_path,
    );

    let width: usize = msg
        .split("\n")
        .collect::<Vec<&str>>()
        .iter()
        .map(|m| m.len())
        .fold(0, std::cmp::max);

    let mut writer = BufWriter::new(stdout());
    say(msg.as_bytes(), width, &mut writer).unwrap();
}

fn daemonize_arconc() {
    let out = format!("{}/arconc.out", DEFAULT_LOG_DIR);
    let err = format!("{}/arconc.err", DEFAULT_LOG_DIR);
    let stdout = std::fs::File::create(&out).unwrap();
    let stderr = std::fs::File::create(&err).unwrap();

    let d = daemonize::Daemonize::new()
        .stdout(stdout)
        .stderr(stderr)
        .working_directory(".");

    match d.start() {
        Ok(_) => info!("arconc running as daemon"),
        Err(e) => error!("Error: {}", e),
    }
}
