// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_error::*;
use hocon::HoconLoader;
use kompact::prelude::KompactConfig;
use serde::Deserialize;

/// Configuration for an Arcon Pipeline
#[derive(Deserialize, Default, Clone, Debug)]
pub struct ArconConf {
    /// Base directory for checkpoints
    #[serde(default = "checkpoint_dir_default")]
    checkpoint_dir: String,
    /// Generation interval in milliseconds for Watermarks at sources
    #[serde(default = "watermark_interval_default")]
    watermark_interval: u64,
    /// Amount of threads for Kompact's threadpool
    #[serde(default = "kompact_threads_default")]
    kompact_threads: usize,
    /// Controls the amount of messages a component processes per schedule iteration
    #[serde(default = "kompact_throughput_default")]
    kompact_throughput: usize,
    /// Float value that sets message priority
    #[serde(default = "kompact_msg_priority_default")]
    kompact_msg_priority: f32,
}

impl ArconConf {
    /// Returns a KompactConfig based on loaded ArconConf
    pub fn kompact_conf(&self) -> KompactConfig {
        let mut cfg = KompactConfig::default();
        cfg.threads(self.kompact_threads);
        cfg.throughput(self.kompact_throughput);
        cfg.msg_priority(self.kompact_msg_priority);
        cfg
    }

    /// Loads ArconConf from a file
    pub fn from_file(path: &str) -> ArconResult<ArconConf> {
        let data = std::fs::read_to_string(path)
            .map_err(|e| arcon_err_kind!("Failed to read config file with err {}", e))?;

        let loader: HoconLoader = HoconLoader::new()
            .load_str(&data)
            .map_err(|e| arcon_err_kind!("Failed to load Hocon Loader with err {}", e))?;

        let conf = loader
            .resolve()
            .map_err(|e| arcon_err_kind!("Failed to resolve ArconConf with err {}", e))?;
        Ok(conf)
    }
}

// Default values

fn checkpoint_dir_default() -> String {
    String::from("/tmp/arcon")
}

fn watermark_interval_default() -> u64 {
    // in milliseconds
    250
}

fn kompact_threads_default() -> usize {
    num_cpus::get()
}

fn kompact_throughput_default() -> usize {
    1
}

fn kompact_msg_priority_default() -> f32 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::prelude::*;
    use tempfile::NamedTempFile;

    #[test]
    fn conf_from_file_test() {
        // Set up Config File
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();
        let config_str = r#"{checkpoint_dir: /dev/null, watermark_interval: 1000}"#;
        file.write_all(config_str.as_bytes()).unwrap();

        // Load conf
        let conf: ArconConf = ArconConf::from_file(&file_path).unwrap();

        // Check custom values
        assert_eq!(conf.checkpoint_dir, String::from("/dev/null"));
        assert_eq!(conf.watermark_interval, 1000);
        // Check defaults
        assert_eq!(conf.kompact_threads, kompact_threads_default());
        assert_eq!(conf.kompact_throughput, kompact_throughput_default());
        assert_eq!(conf.kompact_msg_priority, kompact_msg_priority_default());
    }
}
