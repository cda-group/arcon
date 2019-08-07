use arcon_spec::CompileMode;
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

pub fn cargo_build(mode: &CompileMode) -> Result<(), failure::Error> {
    let mut args: Vec<&str> = vec!["+nightly", "build"];
    if mode == &CompileMode::Release {
        args.push("--release");
    }
    let mut cmd = Command::new("cargo")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .args(args)
        .spawn()
        .unwrap_or_else(|e| panic!("failed to execute process: {}", e));

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
