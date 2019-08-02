use std::env;

fn main() {
    let proto_feature = env::var_os("CARGO_FEATURE_PROTO").is_some();
    let capnproto_feature = env::var_os("CARGO_FEATURE_CAPNPROTO").is_some();

    if proto_feature == capnproto_feature {
        panic!("Either use feature proto or capnproto");
    }

    if proto_feature {
        #[cfg(feature = "proto")]
        protoc_rust::run(protoc_rust::Args {
            out_dir: "src/protobuf/",
            includes: &["./proto"],
            input: &["./proto/messages.proto"],
            customize: protoc_rust::Customize {
                ..Default::default()
            },
        })
        .expect("protoc");
    } else if capnproto_feature {
        #[cfg(feature = "capnproto")]
        ::capnpc::CompilerCommand::new()
            .src_prefix("schema")
            .file("schema/messages.capnp")
            .edition(::capnpc::RustEdition::Rust2018)
            .run()
            .expect("compiling schema");
    } else {
        panic!("Huh, what are you doing?");
    }
}
