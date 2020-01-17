// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate cfg_if;

cfg_if::cfg_if! {
    if #[cfg(feature = "arcon_spec")] {
        pub mod arcon_spec {
            include!(concat!(env!("OUT_DIR"), "/arcon_spec.rs"));

            use failure::Fail;
            use arcon_spec::Mode;

            #[derive(Debug, Fail)]
            #[fail(display = "Loading spec err: `{}`", msg)]
            pub struct SpecError {
                msg: String,
            }

            pub fn spec_from_file(path: &str) -> Result<ArconSpec, SpecError> {
                let file = std::fs::File::open(path).map_err(|e| SpecError { msg: e.to_string() })?;
                serde_json::from_reader(file).map_err(|e| SpecError { msg: e.to_string() })
            }

            pub fn spec_from_bytes(bytes: &[u8]) -> Result<ArconSpec, SpecError> {
                serde_json::from_slice(bytes).map_err(|e| SpecError { msg: e.to_string() })
            }

            pub fn get_compile_mode(spec: &ArconSpec) -> String {
                match spec.mode.as_ref() {
                    Some(Mode::Debug(_)) => "debug".to_string(),
                    Some(Mode::Release(_)) => "release".to_string(),
                    None => "release".to_string(),
                }
            }
        }
    }

}

cfg_if::cfg_if! {
    if #[cfg(feature = "arconc")] {
        pub mod proto {
            include!(concat!(env!("OUT_DIR"), "/mod.rs"));
        }
        pub use proto::arconc::*;
        pub use proto::arconc_grpc::*;
    }
}
