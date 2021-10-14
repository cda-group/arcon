use hocon::HoconLoader;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Deserialize, Clone, Debug)]
pub struct ControlPlaneConf {
    pub sock_addr: Option<String>,
    #[serde(default = "dir_default")]
    pub dir: PathBuf,
}

impl ControlPlaneConf {
    /// Loads ControPlaneConf from a file
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        let data = std::fs::read_to_string(path).unwrap();

        let loader: HoconLoader = HoconLoader::new().load_str(&data).unwrap();

        loader.resolve().unwrap()
    }
}

impl Default for ControlPlaneConf {
    fn default() -> Self {
        Self {
            #[cfg(not(test))]
            sock_addr: Some("127.0.0.1:5000".to_string()),
            #[cfg(test)]
            sock_addr: None,
            dir: dir_default(),
        }
    }
}

fn dir_default() -> PathBuf {
    #[cfg(test)]
    let mut res = tempfile::tempdir().unwrap().into_path();
    #[cfg(not(test))]
    let mut res = std::env::temp_dir();

    res.push("control_plane");
    res
}
