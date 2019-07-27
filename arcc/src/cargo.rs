#![allow(dead_code)]

use cargo::core::{compiler::CompileMode, Workspace};
use cargo::ops::{self, CompileOptions};
use cargo::util::config::Config;
use std::path::Path;

pub fn is_workspace(path: &str) -> bool {
    let path = Path::new(path);
    Workspace::new(&path, &Config::default().unwrap()).is_ok()
}

/// Naive compilation
pub fn compile(path: &str) -> Result<(), failure::Error> {
    let path = Path::new(path);
    assert!(std::env::set_current_dir(&path).is_ok());
    let path = Path::new(path);
    let config = Config::default()?;
    let ws = Workspace::new(&path, &config)?;
    let compile_options = CompileOptions::new(&config, CompileMode::Build)?;
    let _result = ops::compile(&ws, &compile_options).map(|_| ()).unwrap_err();
    Ok(())
}

/// Creates a Workspace member with a Cargo.toml and src/ directory
pub fn create_workspace_member(ws_path: &str, id: &str) -> Result<(), failure::Error> {
    let full_path = format!("{}/{}", ws_path, id);

    let manifest = format!(
        "[package] \
         \nname = \"{}\" \
         \nversion = \"0.1.0\" \
         \nauthors = [\"Arcon Developers <insert-email>\"] \
         \nedition = \"2018\" \
         \n[dependencies] \
         \narcon = {{path = \"../../arcon\"}}",
        id
    );

    let path = format!("{}/src/", full_path);
    std::fs::create_dir_all(path)?;

    let manifest_file = format!("{}/Cargo.toml", full_path);
    codegen::to_file(manifest, manifest_file)?;

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
