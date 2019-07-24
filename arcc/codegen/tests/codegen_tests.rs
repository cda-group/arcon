extern crate arcon;
extern crate codegen;
extern crate compiletest_rs as compiletest;

use codegen::*;
use std::fs;
use std::path::PathBuf;

pub const RUN_PASS_MODE: &str = "run-pass";
pub const RUN_PASS_PATH: &str = "tests/run-pass";

fn run_mode(mode: &str) {
    let mut config = compiletest::Config::default().tempdir();
    let cfg_mode = mode.parse().expect("Invalid mode");

    config.mode = cfg_mode;
    config.src_base = PathBuf::from(format!("tests/{}", mode));
    config.target_rustcflags = Some("-L ../target/debug -L ../target/debug/deps".to_string());

    compiletest::run_tests(&config);
}

fn add_empty_main(path: &str) {
    let main = "fn main() {}";
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(path)
        .unwrap();

    let _ = writeln!(file, "{}", main);
}

#[test]
fn codegen_test() {
    // makes sure that tests/run-pass does not exist
    let _ = fs::remove_dir_all(RUN_PASS_PATH);

    // Fresh start of run-pass tests
    fs::create_dir_all(RUN_PASS_PATH).unwrap();

    basic_system();
    basic_source_map_sink();

    // TODO: Add more complex tests

    run_mode(RUN_PASS_MODE);

    fs::remove_dir_all(RUN_PASS_PATH).unwrap();
}

fn basic_system() {
    let sys = system::system("127.0.0.1:3000", None, None, None);
    let main = generate_main(sys);
    let main_fmt = format_code(main.to_string()).unwrap();
    let sys_path = format!("{}/basic_system.rs", RUN_PASS_PATH);
    let _ = to_file(main_fmt, sys_path.to_string());
}

fn basic_source_map_sink() {
    let sink = crate::sink::sink("sink", "i32", &spec::SinkType::Debug);
    let map = crate::stream_task::stream_task("map", "sink", "|x: i32| x + 5", "i32", "i32");
    let source = crate::source::source(
        "source",
        "map",
        "i32",
        &spec::SourceType::Socket {
            host: "127.0.0.1".to_string(),
            port: 3000,
        },
    );

    let s1 = combine_streams(sink, map);
    let final_stream = combine_streams(s1, source);

    let system = crate::system::system("127.0.0.1:2000", None, Some(final_stream), None);

    let main = generate_main(system);
    let fmt = format_code(main.to_string()).unwrap();
    let sys_path = format!("{}/basic_source_map_sink.rs", RUN_PASS_PATH);
    let _ = to_file(fmt, sys_path.to_string());
}
