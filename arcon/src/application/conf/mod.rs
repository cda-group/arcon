pub mod logger;

use hocon::HoconLoader;
use kompact::{
    net::buffers::BufferConfig,
    prelude::{DeadletterBox, KompactConfig, NetworkConfig, LocalDispatcher},
};
use logger::{file_logger, term_logger, ArconLogger, LoggerType};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Types of modes that `arcon` may run in
#[derive(Deserialize, Clone, Debug)]
pub enum ExecutionMode {
    Local,
    Distributed(DistributedConf),
}
#[derive(Deserialize, Clone, Debug)]
pub struct DistributedConf {
    peers: Vec<String>, // ["192.168.1.1:2000",  "192.168.1.2:2000"]
}

#[derive(Deserialize, Clone, Debug)]
pub enum ControlPlaneMode {
    Embedded,
    Remote(String),
}

/// Configuration for an Arcon Application
#[derive(Deserialize, Clone, Debug)]
pub struct ApplicationConf {
    #[serde(default = "app_name_default")]
    pub app_name: String,
    /// Either a `Local` or `Distributed` Execution Mode
    #[serde(default = "execution_mode_default")]
    pub execution_mode: ExecutionMode,
    /// Control Plane config
    #[serde(default = "control_plane_default")]
    pub control_plane_mode: ControlPlaneMode,
    /// Base directory for the application
    #[serde(default = "base_dir_default")]
    pub base_dir: PathBuf,
    /// [LoggerType] for arcon related logging
    #[serde(default)]
    pub arcon_logger_type: LoggerType,
    /// [LoggerType] for kompact related logging
    #[serde(default)]
    pub kompact_logger_type: LoggerType,
    /// Generation interval in milliseconds for Epochs
    #[serde(default = "epoch_interval_default")]
    pub epoch_interval: u64,
    /// Generation interval in milliseconds for Watermarks at sources
    #[serde(default = "watermark_interval_default")]
    pub watermark_interval: u64,
    /// Arcon Process ID
    #[serde(default)]
    pub arcon_pid: u64,
    /// The highest possible key value for a keyed stream
    ///
    /// This should not be set too low or ridiculously high
    #[serde(default = "max_key_default")]
    pub max_key: u64,
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
    /// Max amount of bytes allowed to be allocated by the Arcon Allocator
    #[serde(default = "allocator_capacity_default")]
    pub allocator_capacity: usize,
    #[serde(default = "ctrl_system_host_default")]
    pub ctrl_system_host: Option<String>,
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
    #[serde(default = "kompact_chunk_size_default")]
    pub kompact_chunk_size: usize,
    #[serde(default = "kompact_initial_chunk_count_default")]
    pub kompact_initial_chunk_count: usize,
    #[serde(default = "kompact_max_chunk_count_default")]
    pub kompact_max_chunk_count: usize,
    #[serde(default = "kompact_encode_buf_min_free_space_default")]
    pub kompact_encode_buf_min_free_space: usize,
}
impl Default for ApplicationConf {
    fn default() -> Self {
        ApplicationConf {
            app_name: app_name_default(),
            execution_mode: execution_mode_default(),
            control_plane_mode: control_plane_default(),
            base_dir: base_dir_default(),
            arcon_logger_type: Default::default(),
            kompact_logger_type: Default::default(),
            watermark_interval: watermark_interval_default(),
            epoch_interval: epoch_interval_default(),
            arcon_pid: Default::default(),
            max_key: max_key_default(),
            node_metrics_interval: node_metrics_interval_default(),
            buffer_pool_size: buffer_pool_size_default(),
            buffer_pool_limit: buffer_pool_limit_default(),
            channel_batch_size: channel_batch_size_default(),
            allocator_capacity: allocator_capacity_default(),
            ctrl_system_host: ctrl_system_host_default(),
            kompact_threads: kompact_threads_default(),
            kompact_throughput: kompact_throughput_default(),
            kompact_msg_priority: kompact_msg_priority_default(),
            kompact_network_host: kompact_network_host_default(),
            kompact_chunk_size: kompact_chunk_size_default(),
            kompact_max_chunk_count: kompact_max_chunk_count_default(),
            kompact_initial_chunk_count: kompact_initial_chunk_count_default(),
            kompact_encode_buf_min_free_space: kompact_encode_buf_min_free_space_default(),
        }
    }
}

impl ApplicationConf {
    pub fn state_dir(&self) -> PathBuf {
        let mut buf = self.base_dir.clone();
        buf.push("live_states");
        buf
    }
    pub fn checkpoints_dir(&self) -> PathBuf {
        let mut buf = self.base_dir.clone();
        buf.push("checkpoints");
        buf
    }

    pub fn arcon_logger(&self) -> ArconLogger {
        match self.arcon_logger_type {
            LoggerType::File => {
                let base_dir = self.base_dir.clone();
                let path = format!(
                    "{}/{}",
                    base_dir.as_path().to_string_lossy(),
                    logger::ARCON_LOG_NAME
                );
                file_logger(&path)
            }
            LoggerType::Terminal => term_logger(),
        }
    }

    fn kompact_logger(&self) -> Option<kompact::KompactLogger> {
        match self.kompact_logger_type {
            LoggerType::File => {
                let base_dir = self.base_dir.clone();
                let path = format!(
                    "{}/{}",
                    base_dir.as_path().to_string_lossy(),
                    logger::KOMPACT_LOG_NAME,
                );
                Some(file_logger(&path))
            }
            LoggerType::Terminal => None,
        }
    }
    pub(crate) fn ctrl_system_conf(&self) -> KompactConfig {
        let mut cfg = KompactConfig::default();

        cfg.set_config_value(
            &kompact::config_keys::system::LABEL,
            "ctrl_system".to_string(),
        );

        // inject checkpoint_dir into Kompact
        let component_cfg = format!(
            "{{ checkpoint_dir = {:?}, node_metrics_interval = {} }}",
            self.checkpoints_dir(),
            self.node_metrics_interval
        );

        if let Some(kompact_logger) = self.kompact_logger() {
            cfg.logger(kompact_logger);
        }

        cfg.load_config_str(component_cfg);

        if let Some(host) = &self.ctrl_system_host {
            let sock_addr = host.parse().unwrap();
            cfg.system_components(DeadletterBox::new, NetworkConfig::new(sock_addr).build());
        } else {
            cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        }

        cfg
    }

    /// Returns a KompactConfig based on loaded ApplicationConf
    pub fn data_system_conf(&self) -> KompactConfig {
        let mut cfg = KompactConfig::default();

        cfg.set_config_value(
            &kompact::config_keys::system::LABEL,
            "data_system".to_string(),
        );

        // inject checkpoint_dir into Kompact
        let component_cfg = format!(
            "{{ checkpoint_dir = {:?}, node_metrics_interval = {} }}",
            self.checkpoints_dir(),
            self.node_metrics_interval
        );

        if let Some(kompact_logger) = self.kompact_logger() {
            cfg.logger(kompact_logger);
        }
        cfg.load_config_str(component_cfg);
        cfg.set_config_value(&kompact::config_keys::system::THREADS, self.kompact_threads);
        cfg.set_config_value(
            &kompact::config_keys::system::THROUGHPUT,
            self.kompact_throughput,
        );
        cfg.set_config_value(
            &kompact::config_keys::system::MESSAGE_PRIORITY,
            self.kompact_msg_priority,
        );

        // Set up Kompact network only if we are gonna use it..
        if let Some(host) = &self.kompact_network_host {
            let mut buffer_config = BufferConfig::default();

            buffer_config.chunk_size(self.kompact_chunk_size);
            buffer_config.max_chunk_count(self.kompact_max_chunk_count);
            buffer_config.initial_chunk_count(self.kompact_initial_chunk_count);
            buffer_config.encode_buf_min_free_space(self.kompact_encode_buf_min_free_space);

            let sock_addr = host.parse().unwrap();
            cfg.system_components(
                DeadletterBox::new,
                NetworkConfig::with_buffer_config(sock_addr, buffer_config).build(),
            );
        } else {
            cfg.system_components(
                DeadletterBox::new,
                NetworkConfig::default().build(),
            );
        }

        cfg
    }

    /// Loads ApplicationConf from a file
    pub fn from_file(path: impl AsRef<Path>) -> ApplicationConf {
        let data = std::fs::read_to_string(path).unwrap();

        let loader: HoconLoader = HoconLoader::new().load_str(&data).unwrap();

        loader.resolve().unwrap()
    }
}

// Default values
fn app_name_default() -> String {
    Uuid::new_v4().to_string()
}

fn execution_mode_default() -> ExecutionMode {
    ExecutionMode::Local
}

fn base_dir_default() -> PathBuf {
    #[cfg(test)]
    let mut res = tempfile::tempdir().unwrap().into_path();
    #[cfg(not(test))]
    let mut res = std::env::temp_dir();

    res.push("arcon");

    res
}

fn control_plane_default() -> ControlPlaneMode {
    ControlPlaneMode::Embedded
}

fn epoch_interval_default() -> u64 {
    // in milliseconds
    25000
}

fn watermark_interval_default() -> u64 {
    // in milliseconds
    250
}

fn max_key_default() -> u64 {
    1024
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

fn allocator_capacity_default() -> usize {
    // 5 GB
    5368709120
}

fn kompact_threads_default() -> usize {
    std::cmp::max(1, num_cpus::get())
}

fn ctrl_system_host_default() -> Option<String> {
    None
}

fn kompact_throughput_default() -> usize {
    25
}

fn kompact_msg_priority_default() -> f32 {
    0.5
}

fn kompact_network_host_default() -> Option<String> {
    None
}

fn kompact_chunk_size_default() -> usize {
    128000
}

fn kompact_max_chunk_count_default() -> usize {
    128
}

fn kompact_initial_chunk_count_default() -> usize {
    2
}

fn kompact_encode_buf_min_free_space_default() -> usize {
    64
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
        let config_str = r#"{base_dir: /dev/null, watermark_interval: 1000}"#;
        file.write_all(config_str.as_bytes()).unwrap();

        // Load conf
        let conf: ApplicationConf = ApplicationConf::from_file(&file_path);

        // Check custom values
        assert_eq!(conf.base_dir, PathBuf::from("/dev/null"));
        assert_eq!(conf.watermark_interval, 1000);
        // Check defaults
        assert_eq!(conf.node_metrics_interval, node_metrics_interval_default());
        assert_eq!(conf.channel_batch_size, channel_batch_size_default());
        assert_eq!(conf.buffer_pool_size, buffer_pool_size_default());
        assert_eq!(conf.allocator_capacity, allocator_capacity_default());
        assert_eq!(conf.kompact_threads, kompact_threads_default());
        assert_eq!(conf.kompact_throughput, kompact_throughput_default());
        assert_eq!(conf.kompact_network_host, kompact_network_host_default());
    }
}
