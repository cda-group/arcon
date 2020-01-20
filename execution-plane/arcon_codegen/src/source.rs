// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::common::*;
use crate::spec::source::SourceKind;
use crate::spec::Source;
use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};
use std::collections::HashSet;

pub fn source(
    id: u32,
    target: &str,
    source: &Source,
    spec_id: &str,
    ts_extractor: u32,
    features: &mut HashSet<String>,
) -> TokenStream {
    let source_name = id_to_ident(id);

    if target.is_empty() {
        panic!("Source is missing target");
    }

    let target = Ident::new(&target, Span::call_site());
    let input_type = to_token_stream(&source.source_type.clone().unwrap(), spec_id);

    match source.source_kind.as_ref() {
        Some(SourceKind::Socket(sock)) => {
            // Add so
            features.insert("socket".to_string());

            socket_source(
                &source_name,
                &target,
                &input_type,
                &sock.addr,
                &sock.protocol,
                source.source_rate,
                ts_extractor,
                id,
            )
        }
        Some(SourceKind::LocalFile(l)) => local_file_source(
            &source_name,
            &target,
            &input_type,
            &l.path,
            source.source_rate,
            id,
        ),
        None => {
            quote! {}
        }
    }
}

fn socket_source(
    source_name: &Ident,
    target: &Ident,
    input_type: &TokenStream,
    addr: &str,
    protocol: &str,
    rate: u64,
    ts_extraction: u32,
    id: u32,
) -> TokenStream {
    let verify = verify_and_start(source_name, "system");

    let ts_quote = quote! { Some(#ts_extraction) };
    let sock_kind = {
        if protocol == "tcp" || protocol == "TCP" {
            quote! { SocketKind::Tcp }
        } else if protocol == "udp" || protocol == "UDP" {
            quote! { SocketKind::Udp}
        } else {
            panic!("Unsupported protocol");
        }
    };

    quote! {
        let actor_ref: ActorRefStrong<ArconMessage<#input_type>> = #target.actor_ref().hold().expect("failed to fetch actor ref");
        let channel = Channel::Local(actor_ref);
        let channel_strategy: Box<ChannelStrategy<#input_type>> = Box::new(Forward::new(channel));
        let (#source_name, reg) = system.create_and_register(move || {
            let sock_addr = #addr.parse().expect("Failed to parse SocketAddr");
            let source: SocketSource<#input_type> = SocketSource::new(sock_addr, #sock_kind, channel_strategy, #rate, #ts_quote, #id.into());
            source
        });

        #verify
    }
}

fn local_file_source(
    source_name: &Ident,
    target: &Ident,
    input_type: &TokenStream,
    file_path: &str,
    rate: u64,
    id: u32,
) -> TokenStream {
    let verify = verify_and_start(source_name, "system");

    quote! {
        let actor_ref: ActorRefStrong<ArconMessage<#input_type>> = #target.actor_ref().hold().expect("failed to fetch actor ref");
        let channel = Channel::Local(actor_ref);
        let channel_strategy: Box<ChannelStrategy<#input_type>> = Box::new(Forward::new(channel));
        let (#source_name, reg) = system.create_and_register(move || {
            let source: LocalFileSource<#input_type> = LocalFileSource::new(
                String::from(#file_path),
                channel_strategy,
                #rate,
                #id.into(),
            );
            source
        });

        #verify
    }
}
