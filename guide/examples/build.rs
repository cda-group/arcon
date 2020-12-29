fn main() -> Result<(), Box<dyn std::error::Error>> {
    arcon_build::compile_protos(&["src/event.proto"], &["src/"]).unwrap();
    Ok(())
}
