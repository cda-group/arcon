extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .src_prefix("schema") // 1
        .file("schema/messages.capnp") // 2
        .edition(::capnpc::RustEdition::Rust2018)
        .run()
        .expect("compiling schema");
}
