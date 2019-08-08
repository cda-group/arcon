#![allow(dead_code)]

use failure::Fail;
use serde::{Deserialize, Serialize};
use std::fs;
use arcon_spec::ArcSpec;

#[derive(Debug, Fail)]
#[fail(display = "CompilerEnv: `{}`", msg)]
pub struct CompileEnvError {
    msg: String,
}

/// Sets up a virtual cargo workspace in order to
/// share dependencies between compiled Arcon binaries
pub struct CompilerEnv {
    pub root: String,
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
                debug!("Using existing workspace: {}", root);
                config
            } else {
                debug!("Creating new workspace at path {}", root);;
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

    pub fn add_project(&mut self, id: String) -> Result<(), CompileEnvError> {
        if self.config.workspace.members.contains(&id) {
            let err_msg = format!("Workspace member {} already exists", id);
            Err(CompileEnvError { msg: err_msg }) 
        } else {
            debug!("Adding Workspace member {}", id);
            self.config.workspace.members.push(id.clone());
            self.update_env()
                .map_err(|_| CompileEnvError { msg: "Failed to update compiler env {}".to_string() })
        }
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

    /// Creates a Workspace member with a Cargo.toml and src/ directory
    pub fn create_workspace_member(&self, ws_path: &str, id: &str) -> Result<(), failure::Error> {
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
        arcon_codegen::to_file(manifest, manifest_file)?;

        Ok(())
    }

    pub fn generate(&self, build_dir: &str, spec: &ArcSpec) -> Result<(), failure::Error> {
        let code = arcon_codegen::generate(&spec, false)?;
        let path = format!("{}/{}/src/main.rs", build_dir, spec.id);
        arcon_codegen::to_file(code, path)?;
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
