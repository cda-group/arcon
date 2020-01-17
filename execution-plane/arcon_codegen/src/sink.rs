// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::common::*;
use crate::spec::sink::SinkKind;
use crate::spec::Sink;
use crate::types::to_token_stream;
use proc_macro2::{Ident, TokenStream};

pub fn sink(id: u32, sink: &Sink, spec_id: &str) -> TokenStream {
    let sink_name = id_to_ident(id);
    let input_type = to_token_stream(&sink.sink_type.clone().unwrap(), spec_id);

    match sink.sink_kind.as_ref() {
        Some(SinkKind::Socket(s)) => socket_sink(&sink_name, &input_type, &s.addr, &s.protocol),
        Some(SinkKind::LocalFile(file)) => {
            local_file_sink(&sink_name, &input_type, &file.path, sink.predecessor)
        }
        Some(SinkKind::Debug(_)) => debug_sink(&sink_name, &input_type),
        None => panic!("No sink to codegen"),
    }
}

fn debug_sink(sink_name: &Ident, input_type: &TokenStream) -> TokenStream {
    let verify = verify_and_start(sink_name, "system");
    quote! {
        let (#sink_name, reg)= system.create_and_register(move || {
            let sink: DebugNode<#input_type> = DebugNode::new();
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
    protocol: &str,
) -> TokenStream {
    let verify = verify_and_start(sink_name, "system");

    let sock_sink = {
        if protocol == "udp" || protocol == "UDP" {
            quote! {  SocketSink::udp(sock_addr); }
        } else {
            unimplemented!()
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
