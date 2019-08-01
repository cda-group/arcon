use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};
use spec::SourceKind;
use spec::Type;

pub fn source(
    name: &str,
    target: &str,
    input_type: &Type,
    source_type: &SourceKind,
) -> TokenStream {
    let source_name = Ident::new(&name, Span::call_site());
    let target = Ident::new(&target, Span::call_site());
    let input_type = to_token_stream(input_type);

    let source_stream = match source_type {
        SourceKind::Socket { host, port } => {
            socket_source(&source_name, &target, &input_type, host, *port as usize)
        }
    };

    source_stream
}

fn socket_source(
    source_name: &Ident,
    target: &Ident,
    input_type: &TokenStream,
    _host: &str,
    port: usize,
) -> TokenStream {
    quote! {
        let #source_name = system.create_and_start(move || {
            let source: SocketSource<#input_type> = SocketSource::new(#port, #target.actor_ref());
            source
        });
    }
}
