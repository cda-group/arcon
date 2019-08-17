use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};
use spec::{SocketKind, SourceKind, Type};

pub fn source(
    name: &str,
    target: &str,
    input_type: &Type,
    source_type: &SourceKind,
    spec_id: &String,
) -> TokenStream {
    let source_name = Ident::new(&name, Span::call_site());
    let target = Ident::new(&target, Span::call_site());
    let input_type = to_token_stream(input_type, spec_id);

    let source_stream = match source_type {
        SourceKind::Socket { addr, kind, rate } => {
            socket_source(&source_name, &target, &input_type, addr, kind, *rate)
        }
        SourceKind::LocalFile { path } => {
            local_file_source(&source_name, &target, &input_type, &path)
        }
    };

    source_stream
}

fn socket_source(
    source_name: &Ident,
    target: &Ident,
    input_type: &TokenStream,
    addr: &str,
    kind: &SocketKind,
    rate: u64,
) -> TokenStream {
    let sock_kind = {
        match kind {
            SocketKind::Tcp => quote! { SocketKind::Tcp },
            SocketKind::Udp => quote! { SocketKind::Udp },
        }
    };

    quote! {
        let channel = Channel::Local(#target.actor_ref());
        let channel_strategy: Box<ChannelStrategy<#input_type>> = Box::new(Forward::new(channel));
        let #source_name = system.create_and_start(move || {
            let sock_addr = #addr.parse().expect("Failed to parse SocketAddr");
            let source: SocketSource<#input_type> = SocketSource::new(sock_addr, #sock_kind, channel_strategy, #rate);
            source
        });
    }
}

fn local_file_source(
    source_name: &Ident,
    target: &Ident,
    input_type: &TokenStream,
    file_path: &str,
) -> TokenStream {
    quote! {
        let channel = Channel::Local(#target.actor_ref());
        let channel_strategy: Box<ChannelStrategy<#input_type>> = Box::new(Forward::new(channel));
        let #source_name = system.create_and_start(move || {
            let source: LocalFileSource<#input_type> = LocalFileSource::new(
                String::from(#file_path),
                channel_strategy,
            );
            source
        });
    }
}
