// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_proto::arcon_spec::{get_compile_mode, ArconSpec};
use std::fs::File;
use std::process::{Command, Stdio};

pub fn target_list() -> Result<String, failure::Error> {
    let output = Command::new("rustc")
        .arg("--print")
        .arg("target-list")
        .output()
        .unwrap_or_else(|e| panic!("failed to execute process: {}", e));

    Ok(cmd_output(output))
}

pub fn rustc_version() -> Result<String, failure::Error> {
    let output = Command::new("rustc")
        .arg("--version")
        .output()
        .unwrap_or_else(|e| panic!("failed to execute process: {}", e));

    Ok(cmd_output(output))
}

pub fn cargo_build(spec: &ArconSpec, logged: Option<String>) -> Result<(), failure::Error> {
    let mut args: Vec<&str> = vec!["+nightly", "build"];
    let mode = get_compile_mode(spec);
    if mode == "release" {
        args.push("--release")
    }

    let mut cmd = {
        if let Some(log_dir) = logged {
            let log_path = format!("{}/{}.log", log_dir, spec.id);
            let log_file = File::create(&log_path)?;
            let log_err = log_file.try_clone()?;
            Command::new("cargo")
                .stdout(Stdio::from(log_file))
                .stderr(Stdio::from(log_err))
                .args(args)
                .spawn()
                .unwrap_or_else(|e| panic!("failed to execute process: {}", e))
        } else {
            Command::new("cargo")
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .args(args)
                .spawn()
                .unwrap_or_else(|e| panic!("failed to execute process: {}", e))
        }
    };

    cmd.wait()?;
    Ok(())
}

fn cmd_output(output: std::process::Output) -> String {
    let mut res: String = {
        if output.status.success() {
            String::from_utf8_lossy(&output.stdout).to_string()
        } else {
            String::from_utf8_lossy(&output.stderr).to_string()
        }
    };

    trim_newline(&mut res);
    res
}

fn trim_newline(s: &mut String) {
    if s.ends_with('\n') {
        s.pop();
        if s.ends_with('\r') {
            s.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_list_test() {
        let targets = target_list().unwrap();
        assert!(targets.len() > 0);
    }

    #[test]
    fn rustc_version_test() {
        assert_eq!(rustc_version().is_ok(), true);
    }
}
