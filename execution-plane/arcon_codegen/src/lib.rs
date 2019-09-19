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

mod common;
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

const ARCON_CODEGEN_VERSION: &'static str = env!("CARGO_PKG_VERSION");

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
                    &source,
                    &spec.id,
                    spec.timestamp_extractor,
                ));
            }
            Sink(sink) => {
                stream.push(sink::sink(
                    &node.id,
                    &sink.sink_type,
                    &sink.kind,
                    &spec.id,
                    &sink.predecessor,
                ));
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
    let code_header = generate_header();
    Ok(format!("{}\n{}", code_header, formatted_main))
}

/// Helper function for merging two `TokenStream`s
pub fn combine_token_streams(s1: TokenStream, s2: TokenStream) -> TokenStream {
    quote! {
        #s1 #s2
    }
}

fn generate_header() -> String {
    let disclaimer = format!(
        "// The following code has been generated by arcon_codegen v{}\n\n\
         // Copyright 2019 KTH Royal Institute of Technology and RISE SICS\n\
         // Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met: \n\n\
         // 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer. \n\
         // 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution. \n\
         // 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission. \n\n\
         // THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS \"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,\n\
         // THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,\n\
         // EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,\n\
         // WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n",
        ARCON_CODEGEN_VERSION
    );

    let allows = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        "#![allow(dead_code)]",
        "#![allow(missing_docs)]",
        "#![allow(unused_imports)]",
        "#![allow(unused_results)]",
        "#![allow(bare_trait_objects)]",
    );

    format!("{}\n{}", disclaimer, allows)
}

/// Generates the main file of an Arcon process
pub fn generate_main(stream: TokenStream, messages: Option<TokenStream>) -> TokenStream {
    quote! {
        extern crate arcon;
        use arcon::prelude::*;
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
