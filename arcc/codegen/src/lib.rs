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

/// Generates the main file of the Operator process
pub fn generate_main(stream: TokenStream) -> TokenStream {
    quote! {
        extern crate arcon;
        use arcon::prelude::*;

        fn main() {
            #stream
        }
    }
}

pub fn combine_streams(s1: TokenStream, s2: TokenStream) -> TokenStream {
    quote! {
        #s1 #s2
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
