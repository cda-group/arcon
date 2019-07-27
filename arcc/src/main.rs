#![feature(vec_remove_item)]
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate failure;
#[macro_use]
extern crate lazy_static;

use clap::{App, AppSettings, Arg, SubCommand};
use ferris_says::say;
use spec::ArcSpec;
use std::fs::metadata;
use std::io::{stdout, BufWriter};
use std::str::FromStr;

mod cargo;
mod env;
mod repl;
mod server;
mod util;

arg_enum! {
    #[derive(Debug)]
    pub enum CompilerMode {
        Boring,
        Lagom,
        Fancy,
    }
}

const DEFAULT_SERVER_PORT: i32 = 3000;
const DEFAULT_SERVER_HOST: &str = "127.0.0.1";
const DEFAULT_ARC_INPUT: &str = "spec.json";
const DEFAULT_BUILD_DIR: &str = "build";

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

    let spec_arg = Arg::with_name("s")
        .required(true)
        .default_value(".")
        .takes_value(true)
        .long("spec")
        .short("s")
        .help("Path to Arc specification");

    let port_arg = Arg::with_name("p")
        .takes_value(true)
        .long("Port number")
        .short("p")
        .help("Port for server");

    let host_arg = Arg::with_name("h")
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
        .help("Directory where arcc builds binaries");

    let matches = App::new("Arc Compiler")
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
        .arg(
            Arg::with_name("m")
                .help("Compiler mode")
                .long("mode")
                .short("m")
                .possible_values(&CompilerMode::variants())
                .default_value("Lagom")
                .required(false),
        )
        .subcommand(
            SubCommand::with_name("compile")
                .setting(AppSettings::ColoredHelp)
                .arg(&spec_arg)
                .arg(&build_dir_arg)
                .about("Compile Arc Specification"),
        )
        .subcommand(
            SubCommand::with_name("server")
                .setting(AppSettings::ColoredHelp)
                .arg(&port_arg)
                .arg(&host_arg)
                .arg(&build_dir_arg)
                .about("Launch Arc Compiler in gRPC server mode"),
        )
        .subcommand(
            SubCommand::with_name("repl")
                .setting(AppSettings::ColoredHelp)
                .about("Arc REPL playground"),
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

    let mode = value_t_or_exit!(matches.value_of("m"), CompilerMode);
    let daemonize: bool = matches.is_present("d");

    match matches.subcommand() {
        ("compile", Some(arg_matches)) => {
            let spec_path = arg_matches
                .value_of("s")
                .expect("Should not happen as there is a default");

            let build_dir: &str = arg_matches.value_of("b").unwrap_or(DEFAULT_BUILD_DIR);

            if let Err(err) = compile(spec_path, build_dir, daemonize, mode) {
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

            if let Err(err) = server(host, port, build_dir, daemonize, mode) {
                error!("Error: {} ", err.to_string());
            }
        }
        ("repl", Some(_)) => {
            if let Err(err) = repl(mode) {
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
    daemonize: bool,
    _mode: CompilerMode,
) -> Result<(), failure::Error> {
    let spec_file: String = {
        let md = metadata(&spec_path)?;
        if md.is_file() {
            spec_path.to_string()
        } else {
            (spec_path.to_owned() + "/" + DEFAULT_ARC_INPUT)
        }
    };

    let spec = ArcSpec::load(&spec_file)?;

    if daemonize {
        unimplemented!();
    } else {
        let mut env = env::CompilerEnv::load(build_dir.to_string())?;
        env.add_project(spec.id.clone())?;
        create_workspace_member(build_dir, &spec.id)?;
        generate(build_dir, &spec)?;
        greeting_with_spec(&spec, &bin_path(&spec.id, build_dir, &spec.mode));
        util::cargo_build(true)?;
    }

    Ok(())
}

fn server(
    host: &str,
    port: i32,
    _build_dir: &str,
    _daemonize: bool,
    _mode: CompilerMode,
) -> Result<(), failure::Error> {
    // TODO: start up gRPC server
    greeting("Server is coming soon!");
    server::start_server(host, port);
    Ok(())
}

fn repl(_mode: CompilerMode) -> Result<(), failure::Error> {
    // TODO: start up REPL
    greeting("REPL is coming soon!");
    Ok(())
}

/// Creates a Workspace member with a Cargo.toml and src/ directory
fn create_workspace_member(ws_path: &str, id: &str) -> Result<(), failure::Error> {
    let full_path = format!("{}/{}", ws_path, id);

    let manifest = format!(
        "[package] \
         \nname = \"{}\" \
         \nversion = \"0.1.0\" \
         \nauthors = [\"Arcon Developers <insert-email>\"] \
         \nedition = \"2018\" \
         \n[dependencies] \
         \narcon = {{path = \"../../arcon\"}}",
        id
    );

    let path = format!("{}/src/", full_path);
    std::fs::create_dir_all(path)?;

    let manifest_file = format!("{}/Cargo.toml", full_path);
    codegen::to_file(manifest, manifest_file)?;

    Ok(())
}

fn generate(build_dir: &str, spec: &ArcSpec) -> Result<(), failure::Error> {
    let code = codegen::generate(&spec, false)?;
    let path = format!("{}/{}/src/main.rs", build_dir, spec.id);
    codegen::to_file(code, path)?;
    let path = std::path::Path::new(build_dir);
    // Enter the directory for compilation...
    std::env::set_current_dir(&path)?;
    Ok(())
}

fn bin_path(id: &str, build_dir: &str, mode: &spec::CompileMode) -> String {
    let mode = match mode {
        spec::CompileMode::Debug => "debug",
        spec::CompileMode::Release => "release",
    };

    let mut dir = String::from(build_dir);

    if dir.ends_with("/") {
        dir.pop();
    }

    format!("{}/target/{}/{}", dir, mode, id)
}

fn greeting_with_spec(spec: &ArcSpec, bin_path: &str) {
    let mode = match spec.mode {
        spec::CompileMode::Debug => "debug",
        spec::CompileMode::Release => "release",
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

fn greeting(msg: &str) {
    let width = msg.len();

    let mut writer = BufWriter::new(stdout());
    say(msg.as_bytes(), width, &mut writer).unwrap();
}
