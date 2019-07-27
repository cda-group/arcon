#![allow(dead_code)]

use failure::Fail;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Fail)]
#[fail(display = "Compiler Env Err: `{}`", msg)]
pub struct CompileEnvError {
    msg: String,
}

/// Sets up a virtual cargo workspace in order to
/// share dependencies between compiled Arcon binaries
pub struct CompilerEnv {
    root: String,
    config: Config,
}

impl CompilerEnv {
    pub fn load(root: String) -> Result<CompilerEnv, failure::Error> {
        let manifest_file = format!("{}/Cargo.toml", root);
        let exists = std::path::Path::new(&manifest_file).exists();

        let config = {
            if exists {
                let data = fs::read_to_string(&manifest_file)?;
                let config = toml::from_str(&data)?;
                config
            } else {
                fs::create_dir_all(&root)?;
                let default_manifest = r#"
                    [workspace]
                    members = []
                "#;
                std::fs::write(&manifest_file, default_manifest)?;
                let config = toml::from_str(default_manifest)?;
                config
            }
        };

        Ok(CompilerEnv { root, config })
    }

    pub fn get_projects(&self) -> Vec<String> {
        self.config.workspace.members.clone()
    }

    pub fn add_project(&mut self, id: String) -> Result<(), failure::Error> {
        self.config.workspace.members.push(id.clone());
        self.update_env()
    }

    pub fn remove_project(&mut self, id: String) -> Result<(), failure::Error> {
        let _ = self.config.workspace.members.remove_item(&id);
        self.update_env()
    }

    fn update_env(&mut self) -> Result<(), failure::Error> {
        let toml = toml::to_string(&self.config)?;
        std::fs::write(&self.manifest(), toml)?;
        Ok(())
    }

    fn manifest(&mut self) -> String {
        format!("{}/Cargo.toml", self.root)
    }

    pub fn destroy(&mut self) -> Result<(), failure::Error> {
        let _ = fs::remove_dir_all(&self.root)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    pub workspace: WorkSpace,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkSpace {
    pub members: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_compiler_env_test() {
        let mut env = CompilerEnv::load("testenv".to_string()).unwrap();
        assert_eq!(env.get_projects().len(), 0);
        env.add_project("hej".to_string()).unwrap();
        assert_eq!(env.get_projects().len(), 1);
        env.remove_project("hej".to_string()).unwrap();
        assert_eq!(env.get_projects().len(), 0);
        env.destroy().unwrap();
    }
}
