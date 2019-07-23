#![allow(bare_trait_objects)]
#![recursion_limit = "128"]
#[macro_use]
extern crate quote;
extern crate proc_macro2;
extern crate rustfmt_nightly;

// Public interface
pub mod error;
pub mod system;
pub mod task;

use error::ErrorKind::*;
use error::*;
use proc_macro2::TokenStream;
use rustfmt_nightly::*;
use std::fs;

/// Rustfmt the generated code to make it readable
pub fn format_code(code: String) -> crate::error::Result<String> {
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
            .map_err(|e| Error::new(CodeFmtError(e.to_string())))?;
    }

    String::from_utf8(buf).map_err(|e| Error::new(CodeFmtError(e.to_string())))
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
        use arcon::weld::module::*;

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
