use crate::common::*;
use crate::types::to_token_stream;
use proc_macro2::{Ident, TokenStream};
use spec::{SinkKind, SocketKind, Type};

pub fn sink(
    id: u32,
    input_type: &Type,
    sink_type: &SinkKind,
    spec_id: &String,
    predecessor: u32,
) -> TokenStream {
    let sink_name = id_to_ident(id);
    let input_type = to_token_stream(input_type, spec_id);

    let sink_stream = match sink_type {
        SinkKind::Debug => debug_sink(&sink_name, &input_type),
        SinkKind::LocalFile { path } => {
            local_file_sink(&sink_name, &input_type, &path, predecessor)
        }
        SinkKind::Socket { addr, kind } => socket_sink(&sink_name, &input_type, addr, kind),
    };

    sink_stream
}

fn debug_sink(sink_name: &Ident, input_type: &TokenStream) -> TokenStream {
    let verify = verify_and_start(sink_name, "system");
    quote! {
        let (#sink_name, reg)= system.create_and_register(move || {
            let sink: DebugSink<#input_type> = DebugSink::new();
            sink
        });
        #verify
    }
}

fn local_file_sink(
    sink_name: &Ident,
    input_type: &TokenStream,
    file_path: &str,
    predecessor: u32,
) -> TokenStream {
    let verify = verify_and_start(sink_name, "system");
    quote! {
        let (#sink_name, reg) = system.create_and_register(move || {
            let sink: LocalFileSink<#input_type> = LocalFileSink::new(#file_path, vec!(#predecessor.into()));
            sink
        });
        #verify
    }
}

fn socket_sink(
    sink_name: &Ident,
    input_type: &TokenStream,
    addr: &str,
    kind: &SocketKind,
) -> TokenStream {
    let verify = verify_and_start(sink_name, "system");
    let sock_sink = {
        match kind {
            SocketKind::Tcp => unimplemented!(),
            SocketKind::Udp => quote! {  SocketSink::udp(sock_addr); },
        }
    };

    quote! {
        let (#sink_name, reg) = system.create_and_register(move || {
            let sock_addr = #addr.parse().expect("Failed to parse SocketAddr");
            let sink: SocketSink<#input_type> = #sock_sink
            sink
        });
        #verify
    }
}
