use std::env;
use std::io::Write;

static ARCONC_RS: &[u8] = b"
/// Generated from protobuf.
pub mod arconc;
/// Generated from protobuf.
pub mod arconc_grpc;
";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    let arconc_feature = env::var_os("CARGO_FEATURE_ARCONC").is_some();
    let arcon_spec_feature = env::var_os("CARGO_FEATURE_ARCON_SPEC").is_some();

    if arconc_feature {
        #[cfg(feature = "arconc")]
        protoc_grpcio::compile_grpc_protos(&["proto/arconc/arconc.proto"], &["proto"], &out_dir)
            .expect("Failed to compile gRPC definitions!");
        std::fs::File::create(out_dir + "/mod.rs")?.write_all(ARCONC_RS)?;
    }

    if arcon_spec_feature {
        #[cfg(feature = "arcon_spec")]
        prost_build::compile_protos(&["proto/arcon_spec/arcon_spec.proto"], &["proto/"]).unwrap();
    }

    Ok(())
}
