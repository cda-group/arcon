#![allow(dead_code)]

use cargo::core::{compiler::CompileMode, Workspace};
use cargo::ops::{self, CompileOptions};
use cargo::util::{config::Config};
use std::path::Path;

pub fn is_workspace(path: &str) -> bool {
    let path = Path::new(path);
    Workspace::new(&path, &Config::default().unwrap()).is_ok()
}

/// Naive compilation
pub fn compile(path: &str) -> Result<(), failure::Error> {
    let path = Path::new(path);
    let config = Config::default()?;
    let ws = Workspace::new(&path, &config)?;
    let compile_options = CompileOptions::new(&config, CompileMode::Build)?;
    let _result = ops::compile(&ws, &compile_options).map(|_| ()).unwrap_err();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_workspace_test() {
        let s = compile("./spec");
        println!("{:?}", s);
        assert_eq!(is_workspace("."), false);
    }
}
