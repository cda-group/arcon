// Code is taken from tokio-macros (MIT License) and modified to fit arcon.
// https://github.com/tokio-rs/tokio/blob/master/tokio-macros/src/entry.rs

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, quote_spanned, ToTokens};
use syn::parse::Parser;

// syn::AttributeArgs does not implement syn::Parse
type AttributeArgs = syn::punctuated::Punctuated<syn::NestedMeta, syn::Token![,]>;

#[derive(Default)]
struct ExecutionConfig {
    debug: bool,
    name: Option<String>,
}

impl ExecutionConfig {
    fn set_debug(&mut self, debug: syn::Lit, span: Span) -> Result<(), syn::Error> {
        let debug = parse_bool(debug, span, "debug")?;
        self.debug = debug;
        Ok(())
    }
    fn set_name(&mut self, name: syn::Lit, span: Span) -> Result<(), syn::Error> {
        if self.name.is_some() {
            return Err(syn::Error::new(span, "`name` set multiple times."));
        }

        let name_str = parse_string(name, span, "name")?;
        self.name = Some(name_str);
        Ok(())
    }
}
/// Config used in case of the attribute not being able to build a valid config
const DEFAULT_ERROR_CONFIG: ExecutionConfig = ExecutionConfig {
    debug: false,
    name: None,
};

fn parse_string(int: syn::Lit, span: Span, field: &str) -> Result<String, syn::Error> {
    match int {
        syn::Lit::Str(s) => Ok(s.value()),
        syn::Lit::Verbatim(s) => Ok(s.to_string()),
        _ => Err(syn::Error::new(
            span,
            format!("Failed to parse value of `{}` as string.", field),
        )),
    }
}

fn parse_bool(bool: syn::Lit, span: Span, field: &str) -> Result<bool, syn::Error> {
    match bool {
        syn::Lit::Bool(b) => Ok(b.value),
        _ => Err(syn::Error::new(
            span,
            format!("Failed to parse value of `{}` as bool.", field),
        )),
    }
}

fn parse_config(_: syn::ItemFn, args: AttributeArgs) -> Result<ExecutionConfig, syn::Error> {
    let mut config = ExecutionConfig::default();

    for arg in args {
        match arg {
            syn::NestedMeta::Meta(syn::Meta::NameValue(namevalue)) => {
                let ident = namevalue
                    .path
                    .get_ident()
                    .ok_or_else(|| {
                        syn::Error::new_spanned(&namevalue, "Must have specified ident")
                    })?
                    .to_string()
                    .to_lowercase();
                match ident.as_str() {
                    "debug" => {
                        config.set_debug(
                            namevalue.lit.clone(),
                            syn::spanned::Spanned::span(&namevalue.lit),
                        )?;
                    }
                    "name" => {
                        config.set_name(
                            namevalue.lit.clone(),
                            syn::spanned::Spanned::span(&namevalue.lit),
                        )?;
                    }
                    name => {
                        let msg = format!(
                            "Unknown attribute {} is specified; expected one of: `debug`, `name`",
                            name,
                        );
                        return Err(syn::Error::new_spanned(namevalue, msg));
                    }
                }
            }
            syn::NestedMeta::Meta(syn::Meta::Path(path)) => {
                let name = path
                    .get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(&path, "Must have specified ident"))?
                    .to_string()
                    .to_lowercase();
                let msg = match name.as_str() {
                    "debug" | "multi_thread" => {
                        format!("Set debug mode with #[{}(debug = \"true\")].", "arcon::app",)
                    }
                    name => {
                        format!(
                            "Unknown attribute {} is specified; expected one of: `debug`",
                            name
                        )
                    }
                };
                return Err(syn::Error::new_spanned(path, msg));
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "Unknown attribute inside the macro",
                ));
            }
        }
    }
    Ok(config)
}

fn token_stream_with_error(mut tokens: TokenStream, error: syn::Error) -> TokenStream {
    tokens.extend(TokenStream::from(error.into_compile_error()));
    tokens
}

pub(crate) fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    let input: syn::ItemFn = match syn::parse(item.clone()) {
        Ok(it) => it,
        Err(e) => return token_stream_with_error(item, e),
    };

    let config = AttributeArgs::parse_terminated
        .parse(args)
        .and_then(|args| parse_config(input.clone(), args));

    match config {
        Ok(config) => parse_knobs(input, config),
        Err(e) => token_stream_with_error(parse_knobs(input, DEFAULT_ERROR_CONFIG), e),
    }
}

fn parse_knobs(mut input: syn::ItemFn, _config: ExecutionConfig) -> TokenStream {
    // If type mismatch occurs, the current rustc points to the last statement.
    let (_, last_stmt_end_span) = {
        let mut last_stmt = input
            .block
            .stmts
            .last()
            .map(ToTokens::into_token_stream)
            .unwrap_or_default()
            .into_iter();
        // `Span` on stable Rust has a limitation that only points to the first
        // token, not the whole tokens. We can work around this limitation by
        // using the first/last span of the tokens like
        // `syn::Error::new_spanned` does.
        let start = last_stmt.next().map_or_else(Span::call_site, |t| t.span());
        let end = last_stmt.last().map_or(start, |t| t.span());
        (start, end)
    };

    let body = &input.block;
    let brace_token = input.block.brace_token;
    let (_tail_return, tail_semicolon) = match body.stmts.last() {
        Some(syn::Stmt::Semi(syn::Expr::Return(_), _)) => (quote! { return }, quote! { ; }),
        Some(syn::Stmt::Semi(..)) | Some(syn::Stmt::Local(..)) | None => {
            match &input.sig.output {
                syn::ReturnType::Type(_, ty) if matches!(&**ty, syn::Type::Tuple(ty) if ty.elems.is_empty()) =>
                {
                    (quote! {}, quote! { ; }) // unit
                }
                syn::ReturnType::Default => (quote! {}, quote! { ; }), // unit
                syn::ReturnType::Type(..) => (quote! {}, quote! {}),   // ! or another
            }
        }
        _ => (quote! {}, quote! {}),
    };

    //if let Some(_) = config.name {}

    input.block = syn::parse2(quote_spanned! {last_stmt_end_span=>
        {
            use arcon::prelude::*;
            #[allow(unused_mut)]
            fn run(ext: impl ToBuilderExt) {
                let mut builder = ext.builder();

                builder
                .build()
                .run_and_block();
            }
            run(#body)#tail_semicolon
        }
    })
    .expect("Parsing failure");
    input.block.brace_token = brace_token;

    let result = quote! {
        #input
    };

    result.into()
}
