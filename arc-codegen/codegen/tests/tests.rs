extern crate codegen;
extern crate compiletest_rs as compiletest;

use codegen::*;
use std::fs;
use std::path::PathBuf;

fn run_mode(mode: &str) {
    let mut config = compiletest::Config::default().tempdir();
    let cfg_mode = mode.parse().expect("Invalid mode");

    config.mode = cfg_mode;
    config.src_base = PathBuf::from(format!("tests/{}", mode));
    config.target_rustcflags = Some("-L ../target/debug -L ../target/debug/deps".to_string());
    config.clean_rmeta();

    compiletest::run_tests(&config);
}

#[test]
fn run_pass_tests() {
    let path = "tests/run-pass";
    fs::create_dir_all(path).unwrap();

    // Naive Operator
    let sys = system::system("127.0.0.1:3000", None, None, None);
    let main = generate_main(sys);
    let main_fmt = format_code(main.to_string()).unwrap();
    let sys_path = format!("{}/naive_operator.rs", path);
    let _ = to_file(main_fmt, sys_path.to_string());

    run_mode("run-pass");

    fs::remove_dir_all(path).unwrap();
}
