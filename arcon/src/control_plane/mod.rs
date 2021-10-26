pub mod app;
pub mod conf;
pub mod distributed;

use conf::ControlPlaneConf;
use kompact::prelude::{BufferConfig, DeadletterBox, KompactConfig, KompactSystem, NetworkConfig};

#[derive(Clone)]
pub enum ControlPlaneContainer {
    Embedded(ControlPlane),
    Remote(String),
}

/// Arcon's Control Plane
///
/// May be embedded into local only applications
/// or run in a networked fashion.
#[derive(Clone)]
pub struct ControlPlane {
    /// KompactSystem to drive the execution of control plane components
    system: KompactSystem,
    /// Control Plane Configuration
    conf: ControlPlaneConf,
}

impl ControlPlane {
    pub fn new(conf: ControlPlaneConf) -> Self {
        let mut cfg = KompactConfig::default();

        cfg.set_config_value(
            &kompact::config_keys::system::LABEL,
            "control_plane".to_string(),
        );

        // set up network related config
        if let Some(sock_addr_str) = &conf.sock_addr {
            let sock_addr = sock_addr_str.parse().unwrap();
            let mut buffer_config = BufferConfig::default();
            buffer_config.encode_buf_min_free_space(64000);
            buffer_config.max_chunk_count(1000);
            buffer_config.initial_chunk_count(128);
            buffer_config.chunk_size(256 * 100024);

            cfg.system_components(
                DeadletterBox::new,
                NetworkConfig::with_buffer_config(sock_addr, buffer_config).build(),
            );
        }

        let system = cfg.build().expect("fail");

        Self { system, conf }
    }

    // Just runs it and blocks...
    pub fn run(self) {
        self.system.await_termination();
    }
}
