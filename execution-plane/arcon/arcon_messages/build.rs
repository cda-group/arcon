
fn main() {
    protoc_rust::run(protoc_rust::Args {
        out_dir: "src/protobuf/",
        includes: &["./proto"],
        input: &["./proto/messages.proto"],
        customize: protoc_rust::Customize {
            ..Default::default()
        },
    }).expect("Failed to generate proto");
}
