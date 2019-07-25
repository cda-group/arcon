#![allow(bare_trait_objects)]
#![recursion_limit = "256"]
#[macro_use]
extern crate quote;
extern crate failure;
extern crate proc_macro2;
extern crate rustfmt_nightly;

// Public interface
pub mod sink;
pub mod source;
pub mod stream_task;
pub mod system;

use failure::Fail;
use proc_macro2::TokenStream;
use rustfmt_nightly::*;
use std::fs;

use spec::{ArcSpec, NodeType::Sink, NodeType::Source, NodeType::StreamTask};

#[derive(Debug, Fail)]
#[fail(display = "Codegen err: `{}`", msg)]
pub struct CodegenError {
    msg: String,
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


/// Generates a main.rs by parsing an `ArcSpec`
pub fn generate(spec: &ArcSpec) -> Result<String, CodegenError> {
    let mut nodes = spec.nodes.clone();
    let mut stream: Vec<TokenStream> = Vec::new();
    let mut previous_node: String = String::new();

    // NOTE: We go backwards while generating the code
    //       i.e. Sink to Source
    while !nodes.is_empty() {
        let node = nodes.pop().unwrap();

        match node.node_type {
            Source(source) => {
                stream.push(source::source(
                    &node.id,
                    &previous_node,
                    &node.input_type.unwrap(),
                    &source,
                ));
            }
            Sink(sink) => {
                stream.push(sink::sink(&node.id, &node.input_type.unwrap(), &sink));
            }
            StreamTask => {
                stream.push(stream_task::stream_task(
                    &node.id,
                    &previous_node,
                    &node.weld_code.unwrap(),
                    &node.input_type.unwrap(),
                    &node.output_type.unwrap(),
                ));
            }
        }

        previous_node = node.id.clone();
    }

    let final_stream = stream
        .into_iter()
        .fold(quote! {}, |f, s| combine_token_streams(f, s));

    let system = system::system("127.0.0.1:2000", None, Some(final_stream), None);

    let main = generate_main(system, None);
    let formatted_main = format_code(main.to_string())?;

    Ok(formatted_main.to_string())
}

/// Helper function for merging two `TokenStream`s
pub fn combine_token_streams(s1: TokenStream, s2: TokenStream) -> TokenStream {
    quote! {
        #s1 #s2
    }
}

/// Generates the main file of the Operator process
pub fn generate_main(stream: TokenStream, messages: Option<TokenStream>) -> TokenStream {
    quote! {
        extern crate arcon;
        use arcon::prelude::*;
        use arcon::macros::*;

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
