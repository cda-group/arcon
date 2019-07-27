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
use indicatif::ProgressBar;
use spec::ArcSpec;
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
        .help("Directory containing the Arc specification");

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
    greeting("Wait while I compile for you");

    let spec_file = &(spec_path.to_owned() + "/" + DEFAULT_ARC_INPUT);
    let spec = ArcSpec::load(spec_file)?;

    if daemonize {
        unimplemented!();
    } else {
        let pb = ProgressBar::new(524);
        for _ in 0..524 {
            pb.inc(1);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        pb.finish_with_message("done");
        let mut env = env::CompilerEnv::build(build_dir.to_string()).unwrap();
        let _ = env.add_project(spec.id.clone());
        let s = cargo::create_workspace_member(build_dir, &spec.id);
        let code = codegen::generate(&spec, false).unwrap();
        let path = format!("{}/{}/src/main.rs", build_dir, spec.id);
        println!("generated following code\n{}", code);
        let _ = codegen::to_file(code, path);
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

fn greeting(msg: &str) {
    let width = msg.len();

    let mut writer = BufWriter::new(stdout());
    say(msg.as_bytes(), width, &mut writer).unwrap();
}
