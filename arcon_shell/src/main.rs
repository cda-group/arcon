#[macro_use]
extern crate clap;
#[macro_use]
extern crate prettytable;

pub mod sql;

use arcon::{client::QUERY_MANAGER_NAME, prelude::*};
use clap::{App, Arg};
use rustyline::{error::ReadlineError, Editor};
use sql::{QuerySender, QUERY_SENDER_PATH};
use std::{net::SocketAddr, time::Duration};

const DEFAULT_HOST_SOCK: &str = "127.0.0.1:3000";
const DEFAULT_ARCON_SOCK: &str = "127.0.0.1:2000";
const DEFAULT_REPL_DIR: &str = "/tmp/arcon_shell";

const SHELL_MSG: &str = "
 ▄▄▄       ██▀███   ▄████▄   ▒█████   ███▄    █      ██████  ██░ ██ ▓█████  ██▓     ██▓
▒████▄    ▓██ ▒ ██▒▒██▀ ▀█  ▒██▒  ██▒ ██ ▀█   █    ▒██    ▒ ▓██░ ██▒▓█   ▀ ▓██▒    ▓██▒
▒██  ▀█▄  ▓██ ░▄█ ▒▒▓█    ▄ ▒██░  ██▒▓██  ▀█ ██▒   ░ ▓██▄   ▒██▀▀██░▒███   ▒██░    ▒██░
░██▄▄▄▄██ ▒██▀▀█▄  ▒▓▓▄ ▄██▒▒██   ██░▓██▒  ▐▌██▒     ▒   ██▒░▓█ ░██ ▒▓█  ▄ ▒██░    ▒██░
 ▓█   ▓██▒░██▓ ▒██▒▒ ▓███▀ ░░ ████▓▒░▒██░   ▓██░   ▒██████▒▒░▓█▒░██▓░▒████▒░██████▒░██████▒
 ▒▒   ▓▒█░░ ▒▓ ░▒▓░░ ░▒ ▒  ░░ ▒░▒░▒░ ░ ▒░   ▒ ▒    ▒ ▒▓▒ ▒ ░ ▒ ░░▒░▒░░ ▒░ ░░ ▒░▓  ░░ ▒░▓  ░
  ▒   ▒▒ ░  ░▒ ░ ▒░  ░  ▒     ░ ▒ ▒░ ░ ░░   ░ ▒░   ░ ░▒  ░ ░ ▒ ░▒░ ░ ░ ░  ░░ ░ ▒  ░░ ░ ▒  ░
  ░   ▒     ░░   ░ ░        ░ ░ ░ ▒     ░   ░ ░    ░  ░  ░   ░  ░░ ░   ░     ░ ░     ░ ░
      ░  ░   ░     ░ ░          ░ ░           ░          ░   ░  ░  ░   ░  ░    ░  ░    ░  ░
                   ░
";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("arcon_shell")
        .version(crate_version!())
        .about("Arcon Shell allows you to interact with a running Arcon application")
        .arg(
            Arg::with_name("s")
                .required(false)
                .default_value(DEFAULT_ARCON_SOCK)
                .takes_value(true)
                .short("s")
                .help("Arcon Endpoint Host Address"),
        )
        .arg(
            Arg::with_name("h")
                .required(false)
                .default_value(DEFAULT_HOST_SOCK)
                .takes_value(true)
                .short("h")
                .help("Arcon Shell Host Address"),
        )
        .arg(
            Arg::with_name("dir")
                .required(false)
                .default_value(DEFAULT_REPL_DIR)
                .takes_value(true)
                .short("d")
                .help("Directory for keeping repl history"),
        )
        .get_matches();

    let sock_addr: SocketAddr = matches
        .value_of("h")
        .expect("Has default, should not fail")
        .parse()
        .unwrap();

    let arcon_sock_addr = matches
        .value_of("s")
        .expect("Has default, should not fail")
        .parse()
        .unwrap();

    let repl_dir = matches
        .value_of("dir")
        .expect("Has default, should not fail");

    if !std::path::Path::new(&repl_dir).exists() {
        std::fs::create_dir_all(repl_dir)?;
    }

    let mut cfg = KompactConfig::default();
    cfg.system_components(DeadletterBox::new, NetworkConfig::new(sock_addr).build());

    let system = cfg.build().expect("fail");

    let query_manager: ActorPath = NamedPath::with_socket(Transport::Tcp, arcon_sock_addr, vec![
        QUERY_MANAGER_NAME.into(),
    ])
    .into();

    let query_sender_path: ActorPath =
        NamedPath::with_socket(Transport::Tcp, sock_addr, vec![QUERY_SENDER_PATH.into()]).into();

    let query_sender = system.create(|| QuerySender::new(query_manager, query_sender_path));

    system
        .register_by_alias(&query_sender, QUERY_SENDER_PATH)
        .wait_expect(Duration::from_millis(1000), "never registered");

    system.start(&query_sender);

    let query_sender_ref = query_sender.actor_ref().hold().expect("fail");

    ptable!([SHELL_MSG]);

    let mut repl = Editor::<()>::new();
    let shell_history_path = format!("{}/shell_history.txt", repl_dir);
    let _ = repl.load_history(&shell_history_path);

    loop {
        match repl.readline(">> ") {
            Ok(input) if input == "sql" => {
                let _ = sql::repl(repl_dir, query_sender_ref.clone());
            }
            Ok(input) if input == "help" => {
                print_help();
            }
            Ok(_) => {
                println!("Unknown Command, see \"help\"");
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(_) => {
                break;
            }
        }
    }

    repl.save_history(&shell_history_path)?;

    Ok(())
}

fn print_help() {
    ptable!(["Command", "Description"], ["sql", "Enter SQL repl"]);
}
