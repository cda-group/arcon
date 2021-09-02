fn main() -> Result<(), Box<dyn std::error::Error>> {
    arcon_build::compile_protos(&["src/basic_v3.proto"], &["src/"]).unwrap();
    Ok(())
}
