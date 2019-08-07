extern crate arcon;
extern crate arcon_codegen;
extern crate compiletest_rs as compiletest;

use arcon_codegen::*;
use arcon_spec::*;
use std::fs;
use std::path::PathBuf;

pub const RUN_PASS_MODE: &str = "run-pass";
pub const RUN_PASS_PATH: &str = "tests/run-pass";
pub const SPECIFICATION_PATH: &str = "tests/specifications";

fn run_mode(mode: &str) {
    let mut config = compiletest::Config::default().tempdir();
    let cfg_mode = mode.parse().expect("Invalid mode");

    config.mode = cfg_mode;
    config.src_base = PathBuf::from(format!("tests/{}", mode));
    config.target_rustcflags = Some("-L ../target/debug -L ../target/debug/deps".to_string());

    compiletest::run_tests(&config);
}

fn _add_empty_main(path: &str) {
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

    add_test_spec("basic_dataflow");
    add_test_spec("tumbling_window_dataflow");

    run_mode(RUN_PASS_MODE);

    fs::remove_dir_all(RUN_PASS_PATH).unwrap();
}

fn add_test_spec(name: &str) {
    let json_path = format!("{}/{}.json", SPECIFICATION_PATH, name);
    let spec = ArcSpec::load(&json_path).unwrap();
    let generated_code = generate(&spec, true).unwrap();
    let path = format!("{}/{}.rs", RUN_PASS_PATH, name);
    let _ = to_file(generated_code, path.to_string());
}
