// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_error::*;
use hocon::HoconLoader;
use kompact::prelude::{DeadletterBox, NetworkConfig, KompactConfig};
use serde::Deserialize;
use std::path::PathBuf;

/// Configuration for an Arcon Pipeline
#[derive(Deserialize, Clone, Debug)]
pub struct ArconConf {
    /// Base directory for checkpoints
    #[serde(default = "checkpoint_dir_default")]
    pub checkpoint_dir: PathBuf,
    /// Generation interval in milliseconds for Watermarks at sources
    #[serde(default = "watermark_interval_default")]
    pub watermark_interval: u64,
    /// Interval in milliseconds for sending off metrics from nodes
    #[serde(default = "node_metrics_interval_default")]
    pub node_metrics_interval: u64,
    /// Amount of buffers pre-allocated to a BufferPool
    #[serde(default = "buffer_pool_size_default")]
    pub buffer_pool_size: usize,
    /// A limit for amount of buffers in a BufferPool
    #[serde(default = "buffer_pool_limit_default")]
    pub buffer_pool_limit: usize,
    /// Batch size for channels
    #[serde(default = "channel_batch_size_default")]
    pub channel_batch_size: usize,
    /// Max amount of bytes allowed to be allocated by the ArconAllocator
    #[serde(default = "allocator_capacity_default")]
    pub allocator_capacity: usize,
    /// Size of Network Buffers
    #[serde(default = "network_buffer_size_default")]
    pub network_buffer_size: usize,
    /// Amount of threads for Kompact's threadpool
    #[serde(default = "kompact_threads_default")]
    pub kompact_threads: usize,
    /// Controls the amount of messages a component processes per schedule iteration
    #[serde(default = "kompact_throughput_default")]
    pub kompact_throughput: usize,
    /// Float value that sets message priority
    #[serde(default = "kompact_msg_priority_default")]
    pub kompact_msg_priority: f32,
    /// Host address for the KompactSystem
    ///
    /// It is set as optional as it is not necessary for local deployments
    #[serde(default = "kompact_network_host_default")]
    pub kompact_network_host: Option<String>,
}

impl ArconConf {
    /// Returns a KompactConfig based on loaded ArconConf
    pub fn kompact_conf(&self) -> KompactConfig {
        let mut cfg = KompactConfig::default();
        // inject checkpoint_dir into Kompact
        let component_cfg = format!(
            "{{ checkpoint_dir = {:?}, node_metrics_interval = {} }}",
            self.checkpoint_dir, self.node_metrics_interval
        );
        cfg.load_config_str(component_cfg);
        cfg.threads(self.kompact_threads);
        cfg.throughput(self.kompact_throughput);
        cfg.msg_priority(self.kompact_msg_priority);

        // Set up Kompact network only if we are gonna use it..
        if let Some(host) = &self.kompact_network_host {
            let sock_addr = host.parse().unwrap();
            cfg.system_components(DeadletterBox::new, NetworkConfig::new(sock_addr).build());
        }

        cfg
    }

    /// Returns the default Arcon Configuration
    pub fn default() -> ArconConf {
        ArconConf {
            checkpoint_dir: checkpoint_dir_default(),
            watermark_interval: watermark_interval_default(),
            node_metrics_interval: node_metrics_interval_default(),
            buffer_pool_size: buffer_pool_size_default(),
            network_buffer_size: network_buffer_size_default(),
            buffer_pool_limit: buffer_pool_limit_default(),
            channel_batch_size: channel_batch_size_default(),
            allocator_capacity: allocator_capacity_default(),
            kompact_threads: kompact_threads_default(),
            kompact_throughput: kompact_throughput_default(),
            kompact_msg_priority: kompact_msg_priority_default(),
            kompact_network_host: kompact_network_host_default(),
        }
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

fn checkpoint_dir_default() -> PathBuf {
    let mut res = std::env::temp_dir();
    res.push("arcon");
    res
}

fn watermark_interval_default() -> u64 {
    // in milliseconds
    250
}

fn node_metrics_interval_default() -> u64 {
    // in milliseconds
    250
}

fn buffer_pool_size_default() -> usize {
    1024
}

fn buffer_pool_limit_default() -> usize {
    buffer_pool_size_default() * 2
}

fn channel_batch_size_default() -> usize {
    248
}

fn network_buffer_size_default() -> usize {
    64000
}

fn allocator_capacity_default() -> usize {
    // 500 MB
    524288000
}

fn kompact_threads_default() -> usize {
    std::cmp::max(1, num_cpus::get())
}

fn kompact_throughput_default() -> usize {
    50
}

fn kompact_msg_priority_default() -> f32 {
    1.0
}

fn kompact_network_host_default() -> Option<String> {
    None
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
        assert_eq!(conf.checkpoint_dir, PathBuf::from("/dev/null"));
        assert_eq!(conf.watermark_interval, 1000);
        // Check defaults
        assert_eq!(conf.node_metrics_interval, node_metrics_interval_default());
        assert_eq!(conf.channel_batch_size, channel_batch_size_default());
        assert_eq!(conf.buffer_pool_size, buffer_pool_size_default());
        assert_eq!(conf.network_buffer_size, network_buffer_size_default());
        assert_eq!(conf.allocator_capacity, allocator_capacity_default());
        assert_eq!(conf.kompact_threads, kompact_threads_default());
        assert_eq!(conf.kompact_throughput, kompact_throughput_default());
        assert_eq!(conf.kompact_msg_priority, kompact_msg_priority_default());
        assert_eq!(conf.kompact_network_host, kompact_network_host_default());
    }
}
