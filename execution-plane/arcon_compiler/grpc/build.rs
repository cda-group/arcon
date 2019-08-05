extern crate protoc_grpcio;

fn main() {
    let proto_root = "../../../proto";
    let proto_output = "src";
    println!("cargo:rerun-if-changed={}", proto_root);
    protoc_grpcio::compile_grpc_protos(&["../../../proto/arconc.proto"], &[proto_root], &proto_output)
        .expect("Failed to compile gRPC definitions!");
}
