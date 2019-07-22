#![feature(vec_remove_item)]

mod cargo;
mod env;

use env::CompilerEnv;
use failure::Fail;

#[derive(Debug, Fail)]
#[fail(display = "Compiler Err: `{}`", msg)]
pub struct CompilerErr {
    msg: String,
}

pub struct Compiler {
    _env: CompilerEnv,
}

impl Compiler {
    pub fn new(build_dir: String) -> Result<Compiler, failure::Error> {
        let env: CompilerEnv = CompilerEnv::build(build_dir)?;
        Ok(Compiler { _env: env })
    }
    /// Creates Cargo workspace for the target binary
    pub fn create_workspace(ws_path: &str, id: &str) -> Result<(), failure::Error> {
        let full_path = format!("{}/{}", ws_path, id);

        let manifest = format!(
            "[package] \
             \nname = \"{}\" \
             \nversion = \"0.1.0\" \
             \nauthors = [\"Arcon Developers <insert-email>\"] \
             \nedition = \"2018\" \
             \n[dependencies] \
             \nruntime = {{path = \"../../arcon\"}}",
            id
        );

        let path = format!("{}/src/", full_path);
        std::fs::create_dir_all(path)?;

        let manifest_file = format!("{}/Cargo.toml", full_path);
        codegen::to_file(manifest, manifest_file)?;

        Ok(())
    }
}
