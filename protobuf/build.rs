extern crate cfg_if;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    cfg_if::cfg_if! {
        if #[cfg(feature = "arcon_spec")] {
            let mut config = prost_build::Config::new();
            config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
            config.type_attribute(".", "#[serde(rename_all = \"camelCase\")]");
            config.compile_protos(&["proto/arcon_spec/arcon_spec.proto"], &["proto/"]).unwrap();
        } else if #[cfg(feature = "arconc")] {
            use std::io::Write;
            static ARCONC_RS: &[u8] = b"
            /// Generated from protobuf.
            pub mod arconc;
            /// Generated from protobuf.
            pub mod arconc_grpc;
            ";
            let out_dir = std::env::var("OUT_DIR")?;
            protoc_grpcio::compile_grpc_protos(&["proto/arconc/arconc.proto"], &["proto"], &out_dir)
                .expect("Failed to compile gRPC definitions!");
            std::fs::File::create(out_dir + "/mod.rs")?.write_all(ARCONC_RS)?;
        }
    }

    Ok(())
}
