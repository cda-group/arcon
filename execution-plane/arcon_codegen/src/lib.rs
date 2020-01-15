#![allow(bare_trait_objects)]
#![recursion_limit = "256"]
#[macro_use]
extern crate quote;
extern crate failure;
extern crate proc_macro2;
extern crate rustfmt_nightly;
#[macro_use]
extern crate lazy_static;

mod common;
mod function;
mod sink;
mod source;
mod system;
mod types;
mod window;

pub use arcon_proto::arcon_spec as spec;
use failure::Fail;
use proc_macro2::TokenStream;
use rustfmt_nightly::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::Mutex;

use spec::node::NodeKind;
use spec::ArconSpec;

const ARCON_CODEGEN_VERSION: &str = env!("CARGO_PKG_VERSION");

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
    static ref GENERATED_FUNCTIONS: Mutex<HashMap<String, HashMap<String, String>>> = {
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
pub fn generate(
    spec: &ArconSpec,
    is_terminated: bool,
) -> Result<(String, Vec<String>), CodegenError> {
    let mut nodes = spec.nodes.clone();
    let mut stream: Vec<TokenStream> = Vec::new();
    let mut features: HashSet<String> = HashSet::new();
    let mut previous_node: String = String::new();

    {
        // Initialise structs/funcs for this spec
        let mut struct_map = GENERATED_STRUCTS.lock().unwrap();
        struct_map.insert(spec.id.clone(), HashMap::new());
        let mut fn_map = GENERATED_FUNCTIONS.lock().unwrap();
        fn_map.insert(spec.id.clone(), HashMap::new());
    }

    // NOTE: We go backwards while generating the code
    //       i.e. Sink to Source
    while !nodes.is_empty() {
        let node = nodes.pop().unwrap();

        match node.node_kind.as_ref() {
            Some(NodeKind::Source(source)) => {
                stream.push(source::source(
                    node.id,
                    &previous_node,
                    &source,
                    &spec.id,
                    0, // ts extractor.. fix
                    &mut features,
                ));
            }
            Some(NodeKind::Sink(sink)) => {
                stream.push(sink::sink(node.id, &sink, &spec.id));
            }
            Some(NodeKind::Window(window)) => {
                stream.push(window::window(node.id, &window, &spec.id));
            }
            Some(NodeKind::Function(func)) => {
                stream.push(function::function(
                    node.id,
                    &previous_node,
                    node.parallelism,
                    &func,
                    &spec.id,
                ));
            }
            None => {}
        }
        previous_node = "node".to_string() + &node.id.to_string();
    }

    let final_stream = stream
        .into_iter()
        .fold(quote! {}, combine_token_streams);

    // By default, the system is told to block
    let termination = {
        if is_terminated {
            None
        } else {
            Some(system::await_termination("system"))
        }
    };

    // NOTE: Currently just assumes there is a single KompactSystem
    let system = system::system(&spec.system, None, Some(final_stream), termination);

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

    // Check for functions
    let mut fn_map = GENERATED_FUNCTIONS.lock().unwrap();
    let mut fn_streams: Vec<TokenStream> = Vec::new();
    if let Some(map) = fn_map.get(&spec.id) {
        for (_, v) in map.iter() {
            let stream: proc_macro2::TokenStream = v.parse().unwrap();
            fn_streams.push(stream);
        }
    }

    // Remove generated functions
    let _ = fn_map.remove(&spec.id);

    // Create an optional `TokenStream` with struct definitions
    let structs = {
        if struct_streams.is_empty() {
            None
        } else {
            let defs = struct_streams
                .into_iter()
                .fold(quote! {}, combine_token_streams);
            Some(defs)
        }
    };

    // Create an optional `TokenStream` with functions
    let functions = {
        if fn_streams.is_empty() {
            None
        } else {
            let defs = fn_streams
                .into_iter()
                .fold(quote! {}, combine_token_streams);
            Some(defs)
        }
    };

    let pre_main = {
        match (structs, functions) {
            (Some(s), Some(f)) => Some(combine_token_streams(s, f)),
            (Some(s), None) => Some(s),
            (None, Some(f)) => Some(f),
            (None, None) => None,
        }
    };

    let main = generate_main(system, pre_main);
    let formatted_main = format_code(main.to_string())?;

    if formatted_main.is_empty() {
        let msg_err = format!("Failed to format the following Rust code\n {}", main);
        return Err(CodegenError { msg: msg_err });
    }

    let code_header = generate_header();
    let final_code = format!("{}\n{}", code_header, formatted_main);
    Ok((final_code, features.iter().cloned().collect()))
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
         // Copyright 2019 KTH Royal Institute of Technology\n\
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
pub fn generate_main(stream: TokenStream, pre_main: Option<TokenStream>) -> TokenStream {
    quote! {
        extern crate arcon;
        use arcon::prelude::*;
        use arcon::macros::*;

        #pre_main

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
