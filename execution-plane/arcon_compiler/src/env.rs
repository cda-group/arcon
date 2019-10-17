#![allow(dead_code)]

use arcon_spec::{ArconSpec, CompileMode};
use failure::Fail;
use path_clean::PathClean;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

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
    pub log_dir: Option<String>,
    local_arcon: bool,
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
                debug!("Creating new workspace at path {}", root);
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

        Ok(CompilerEnv {
            root,
            config,
            log_dir: None,
            local_arcon: false,
        })
    }

    pub fn set_local_arcon(&mut self) {
        self.local_arcon = true;
    }
    pub fn add_log_dir(&mut self, dir: String) {
        self.log_dir = Some(dir)
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
                .map_err(|e| CompileEnvError { msg: e.to_string() })
        }
    }

    pub fn remove_project(&mut self, id: String) -> Result<(), failure::Error> {
        let _ = self.config.workspace.members.remove_item(&id);
        self.update_env()
    }

    fn update_env(&mut self) -> Result<(), failure::Error> {
        let toml = toml::to_string(&self.config)?;
        std::fs::write("Cargo.toml", toml)?;
        Ok(())
    }

    /// Creates a Workspace member with a Cargo.toml and src/ directory
    pub fn create_workspace_member(&self, id: &str) -> Result<(), failure::Error> {
        let full_path = format!("{}", id);

        // Check if we are to use a local arcon crate or pull from crates.io
        let arcon_dependency = if self.local_arcon {
            String::from("path = \"../../arcon\"")
        } else {
            format!("version = \"{}\"", crate::ARCON_VER)
        };

        let manifest = format!(
            "[package] \
             \nname = \"{}\" \
             \nversion = \"0.1.0\" \
             \nauthors = [\"Arcon Developers <insert-email>\"] \
             \nedition = \"2018\" \
             \n[dependencies] \
             \narcon = {{{}}} \
             \nserde = {{version = \"1.0.63\", features = [\"derive\"] }}",
            id, arcon_dependency
        );

        let path = format!("{}/src/", full_path);
        std::fs::create_dir_all(path)?;

        let manifest_file = format!("{}/Cargo.toml", full_path);
        arcon_codegen::to_file(manifest, manifest_file)?;

        Ok(())
    }

    pub fn generate(&self, spec: &ArconSpec) -> Result<(), failure::Error> {
        let code = arcon_codegen::generate(&spec, false)?;
        let path = format!("{}/src/main.rs", spec.id);
        arcon_codegen::to_file(code, path)?;
        Ok(())
    }

    pub fn bin_path(&self, id: &str, mode: &CompileMode) -> Result<String, failure::Error> {
        let mode = match mode {
            CompileMode::Debug => "debug",
            CompileMode::Release => "release",
        };

        let path_str = format!("target/{}/{}", mode, id);
        let path = std::path::Path::new(&path_str);
        let abs_path = self.absolute_path(path)?;
        Ok(abs_path.into_os_string().into_string().unwrap())
    }

    fn absolute_path<P>(&self, path: P) -> std::io::Result<PathBuf>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        if path.is_absolute() {
            Ok(path.to_path_buf().clean())
        } else {
            Ok(std::env::current_dir()?.join(path).clean())
        }
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
    use tempfile::TempDir;

    #[test]
    fn simple_compiler_env_test() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();

        let mut env = CompilerEnv::load(dir_path.clone()).unwrap();

        // NOTE: this should probably be approached in a different way
        let path = std::path::Path::new(&dir_path);
        std::env::set_current_dir(&path).unwrap();

        assert_eq!(env.get_projects().len(), 0);
        env.add_project("project".to_string()).unwrap();
        assert_eq!(env.get_projects().len(), 1);
        env.remove_project("project".to_string()).unwrap();
        assert_eq!(env.get_projects().len(), 0);
        env.destroy().unwrap();
    }
}
