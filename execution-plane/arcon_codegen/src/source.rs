use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};
use spec::{SocketKind, Source, SourceKind, TimestampExtraction};

pub fn source(name: &str, target: &str, source: &Source, spec_id: &String) -> TokenStream {
    let source_name = Ident::new(&name, Span::call_site());
    let target = Ident::new(&target, Span::call_site());
    let input_type = to_token_stream(&source.source_type, spec_id);

    let source_stream = match &source.kind {
        SourceKind::Socket { addr, kind } => socket_source(
            &source_name,
            &target,
            &input_type,
            &addr,
            &kind,
            *&source.rate,
            &source.ts_extraction,
        ),
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
    ts_extraction: &Option<TimestampExtraction>,
) -> TokenStream {
    let sock_kind = {
        match kind {
            SocketKind::Tcp => quote! { SocketKind::Tcp },
            SocketKind::Udp => quote! { SocketKind::Udp },
        }
    };

    let ts_quote = {
        match ts_extraction {
            Some(ext) => {
                let i: u32 = ext.index;
                quote! { Some(#i) }
            }
            None => quote! { None },
        }
    };

    quote! {
        let channel = Channel::Local(#target.actor_ref());
        let channel_strategy: Box<ChannelStrategy<#input_type>> = Box::new(Forward::new(channel));
        let #source_name = system.create_and_start(move || {
            let sock_addr = #addr.parse().expect("Failed to parse SocketAddr");
            let source: SocketSource<#input_type> = SocketSource::new(sock_addr, #sock_kind, channel_strategy, #rate, #ts_quote);
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
