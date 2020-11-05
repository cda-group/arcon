// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate arcon;
extern crate arcon_codegen;

use arcon_codegen::*;
use arcon_proto::arcon_spec;
use std::fs;

pub const RUN_PASS_MODE: &str = "run-pass";
pub const RUN_PASS_PATH: &str = "tests/run-pass";
pub const SPECIFICATION_PATH: &str = "tests/specifications";

#[test]
fn codegen_test() {
    // makes sure that tests/run-pass does not exist
    let _ = fs::remove_dir_all(RUN_PASS_PATH);

    // Fresh start of run-pass tests
    fs::create_dir_all(RUN_PASS_PATH).unwrap();

    /*
    // NOTE: Uncomment these once codegen is a bit more stable...
    //let t = trybuild::TestCases::new();

    add_test_spec("basic_dataflow");
    add_test_spec("tumbling_window_dataflow");
    add_test_spec("normalise");
    add_test_spec("pipeline_with_structs");

    // test all generated .rs files
    let specs = format!("{}/{}", RUN_PASS_PATH, "*.rs");
    t.pass(&specs);
    */
}

fn _add_test_spec(name: &str) {
    let json_path = format!("{}/{}.json", SPECIFICATION_PATH, name);
    let spec = arcon_spec::spec_from_file(&json_path).unwrap();
    let (code, _) = generate(&spec, true).unwrap();
    let path = format!("{}/{}.rs", RUN_PASS_PATH, name);
    let _ = to_file(code, path.to_string());
}

fn _add_empty_main(path: &str) {
    let main = "fn main() {}";
    use std::{fs::OpenOptions, io::Write};

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(path)
        .unwrap();

    let _ = writeln!(file, "{}", main);
}
