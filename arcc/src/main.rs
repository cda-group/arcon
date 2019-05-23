#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
#[macro_use]
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
const DEFAULT_ARC_INPUT: &str = "spec.json";

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

    let target_arg = Arg::with_name("t")
        .required(false)
        .takes_value(true)
        .long("target")
        .short("t")
        .help("Target triple (e.g., x86_64-unknown-linux-gnu)");

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
                .arg(&target_arg)
                .about("Compile Arc IR"),
        )
        .subcommand(
            SubCommand::with_name("server")
                .setting(AppSettings::ColoredHelp)
                .arg(&port_arg)
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

            let target: Option<&str> = arg_matches.value_of("t");

            if let Err(err) = compile(spec_path, target, daemonize, mode) {
                error!("Error: {} ", err.to_string());
            }
        }
        ("server", Some(arg_matches)) => {
            let port: i32 = arg_matches
                .value_of("p")
                .and_then(|x| <i32 as FromStr>::from_str(x).ok())
                .unwrap_or(DEFAULT_SERVER_PORT);

            if let Err(err) = server(port, daemonize, mode) {
                error!("Error: {} ", err.to_string());
            }
        }
        ("repl", Some(arg_matches)) => {
            if let Err(err) = repl(mode) {
                error!("Error: {} ", err.to_string());
            }
        }
        ("targets", Some(arg_matches)) => {
            eprintln!("{:#?}", *TARGETS);
        }
        ("rustc", Some(arg_matches)) => {
            eprintln!("{}", *RUSTC_VERSION);
        }
        _ => eprintln!("Command did not match!"),
    }
}

fn fetch_args() -> Vec<String> {
    std::env::args().collect()
}

fn compile(
    path: &str,
    target: Option<&str>,
    daemonize: bool,
    mode: CompilerMode,
) -> Result<(), failure::Error> {
    greeting("Wait while I compile for you");

    let spec_file = &(path.to_owned() + "/" + DEFAULT_ARC_INPUT);
    let spec = ArcSpec::load(spec_file)?;

    // TODO: Send to Arc
    debug!("input: {}", spec.code);

    let pb = ProgressBar::new(524);
    for _ in 0..524 {
        pb.inc(1);
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    pb.finish_with_message("done");

    Ok(())
}

fn server(port: i32, daemonize: bool, mode: CompilerMode) -> Result<(), failure::Error> {
    // TODO: start up gRPC server
    greeting("Server is coming soon!");
    Ok(())
}

fn repl(mode: CompilerMode) -> Result<(), failure::Error> {
    // TODO: start up REPL
    greeting("REPL is coming soon!");
    Ok(())
}

fn greeting(msg: &str) {
    let width = msg.len();

    let mut writer = BufWriter::new(stdout());
    say(msg.as_bytes(), width, &mut writer).unwrap();
}
