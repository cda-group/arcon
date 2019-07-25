use proc_macro2::{Ident, Span, TokenStream};
use spec::SourceType;

pub fn source(name: &str, target: &str, input_type: &str, source_type: &SourceType) -> TokenStream {
    let source_name = Ident::new(&name, Span::call_site());
    let target = Ident::new(&target, Span::call_site());
    let input_type = Ident::new(&input_type, Span::call_site());

    let source_stream = match source_type {
        SourceType::Socket { host, port } => {
            socket_source(&source_name, &target, &input_type, host, *port as usize)
        }
        _ => panic!("Undefined source type!"),
    };

    source_stream
}

fn socket_source(
    source_name: &Ident,
    target: &Ident,
    input_type: &Ident,
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
