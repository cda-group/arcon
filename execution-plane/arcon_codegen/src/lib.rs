#![allow(bare_trait_objects)]
#![recursion_limit = "256"]
#[macro_use]
extern crate quote;
extern crate arcon_spec as spec;
extern crate failure;
extern crate proc_macro2;
extern crate rustfmt_nightly;
#[macro_use]
extern crate lazy_static;

mod sink;
mod source;
mod stream_task;
mod system;
mod types;
mod window;

use failure::Fail;
use proc_macro2::TokenStream;
use rustfmt_nightly::*;
use std::collections::HashMap;
use std::fs;
use std::sync::Mutex;

use spec::ArconSpec;
use spec::NodeKind::{Sink, Source, Task, Window};

#[derive(Debug, Fail)]
#[fail(display = "Codegen err: `{}`", msg)]
pub struct CodegenError {
    msg: String,
}

lazy_static! {
    static ref GENERATED_STRUCTS: Mutex<HashMap<String, HashMap<String, String>>> = {
        let m = HashMap::new();
        Mutex::new(m)
    };
}

/// Rustfmt the generated code to make it readable
pub fn format_code(code: String) -> Result<String, CodegenError> {
    let input = Input::Text(code);
    let mut config = Config::default();
    config.set().newline_style(NewlineStyle::Unix);
    config.set().emit_mode(EmitMode::Stdout);
    config.set().verbose(Verbosity::Quiet);
    config.set().hide_parse_errors(true);

    let mut buf: Vec<u8> = vec![];
    {
        let mut session = Session::new(config, Some(&mut buf));
        let _ = session
            .format(input)
            .map_err(|e| CodegenError { msg: e.to_string() })?;
    }

    String::from_utf8(buf).map_err(|e| CodegenError { msg: e.to_string() })
}

/// Save generated code to file
pub fn to_file(input: String, path: String) -> std::result::Result<(), std::io::Error> {
    fs::write(&path, input)
}

/// Generates a main.rs by parsing an `ArconSpec`
pub fn generate(spec: &ArconSpec, is_terminated: bool) -> Result<String, CodegenError> {
    let mut nodes = spec.nodes.clone();
    let mut stream: Vec<TokenStream> = Vec::new();
    let mut previous_node: String = String::new();

    {
        // Creat entry for this Arcon Spec
        let mut struct_map = GENERATED_STRUCTS.lock().unwrap();
        struct_map.insert(spec.id.clone(), HashMap::new());
    }

    // NOTE: We go backwards while generating the code
    //       i.e. Sink to Source
    while !nodes.is_empty() {
        let node = nodes.pop().unwrap();

        match node.kind {
            Source(source) => {
                stream.push(source::source(
                    &node.id,
                    &previous_node,
                    &source.source_type,
                    &source.kind,
                    &spec.id,
                ));
            }
            Sink(sink) => {
                stream.push(sink::sink(&node.id, &sink.sink_type, &sink.kind, &spec.id));
            }
            Task(task) => {
                stream.push(stream_task::stream_task(
                    &node.id,
                    &previous_node,
                    &node.parallelism,
                    &task,
                    &spec.id,
                ));
            }
            Window(window) => {
                stream.push(window::window(&node.id, &window, &spec.id));
            }
        }

        previous_node = node.id.clone();
    }

    let final_stream = stream
        .into_iter()
        .fold(quote! {}, |f, s| combine_token_streams(f, s));

    // By default, the system is told to block
    let termination = {
        if is_terminated {
            None
        } else {
            Some(system::await_termination("system"))
        }
    };

    // NOTE: Currently just assumes there is a single KompactSystem
    let system = system::system(&spec.system_addr, None, Some(final_stream), termination);

    // Check for struct definitions
    let mut struct_map = GENERATED_STRUCTS.lock().unwrap();
    let mut struct_streams: Vec<TokenStream> = Vec::new();
    if let Some(map) = struct_map.get(&spec.id) {
        for (_, v) in map.iter() {
            let stream: proc_macro2::TokenStream = v.parse().unwrap();
            struct_streams.push(stream);
        }
    }

    // Remove this Arcon specs entry
    let _ = struct_map.remove(&spec.id);

    // Create an optional `TokenStream` with struct definitions
    let struct_definitions = {
        if struct_streams.is_empty() {
            None
        } else {
            let defs = struct_streams
                .into_iter()
                .fold(quote! {}, |f, s| combine_token_streams(f, s));
            Some(defs)
        }
    };

    let main = generate_main(system, struct_definitions);
    let formatted_main = format_code(main.to_string())?;

    Ok(formatted_main.to_string())
}

/// Helper function for merging two `TokenStream`s
pub fn combine_token_streams(s1: TokenStream, s2: TokenStream) -> TokenStream {
    quote! {
        #s1 #s2
    }
}

/// Generates the main file of an Arcon process
pub fn generate_main(stream: TokenStream, messages: Option<TokenStream>) -> TokenStream {
    quote! {
        #![allow(bare_trait_objects)]
        #![allow(dead_code)]
        extern crate arcon;
        use arcon::prelude::*;
        #[allow(unused_imports)]
        use arcon::macros::*;

        #[macro_use]
        extern crate serde;

        #messages

        fn main() {
            #stream
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_fmt_test() {
        let code = "#[derive(ComponentDefinition)] pub struct Task{ctx: ComponentContext}";
        let expected =
            "#[derive(ComponentDefinition)]\npub struct Task {\n    ctx: ComponentContext,\n}\n";
        let formatted = format_code(code.to_string()).unwrap();
        assert_eq!(formatted, expected);
    }
}
