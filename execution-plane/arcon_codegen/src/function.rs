// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    common::{id_to_ident, verify_and_start},
    spec,
    spec::channel_kind::ChannelKind,
    types::to_token_stream,
    GENERATED_FUNCTIONS,
};
use arcon_proto::arcon_spec::{Function, FunctionKind};
use proc_macro2::{Ident, Span, TokenStream};

pub fn function(
    id: u32,
    target_name: &str,
    parallelism: u32,
    func: &Function,
    spec_id: &str,
) -> TokenStream {
    let fn_ident = function_gen(spec_id, &func.id, func.udf.clone());
    let node_id = id;
    let node_name = id_to_ident(id);
    let input_type = to_token_stream(&func.input_type.clone().unwrap(), spec_id);
    let output_type = to_token_stream(&func.output_type.clone().unwrap(), spec_id);

    let successors: &Vec<spec::ChannelKind> = &func.successors;
    let predecessor = func.predecessor;

    if parallelism == 1 {
        assert_eq!(successors.len(), 1);

        match &successors.get(0).unwrap().channel_kind.as_ref() {
            Some(ChannelKind::Local(_)) => {
                let target = Ident::new(target_name, Span::call_site());
                let kind: FunctionKind = unsafe { ::std::mem::transmute(func.kind) };
                let task_signature = {
                    match &kind {
                        FunctionKind::FlatMap => {
                            quote! {
                                FlatMap::<#input_type, #output_type>::new(&#fn_ident)
                            }
                        }
                        FunctionKind::Map => {
                            quote! {
                                Map::<#input_type, #output_type>::new(&#fn_ident)
                            }
                        }
                        FunctionKind::Filter => {
                            quote! {
                                Filter::<#input_type>::new(&#fn_ident)
                            }
                        }
                    }
                };
                let channel_strategy_quote = match &kind {
                    FunctionKind::Filter => {
                        quote! {
                            let channel_strategy: Box<ChannelStrategy<#input_type>> = Box::new(Forward::new(channel));
                        }
                    }
                    _ => {
                        quote! {
                            let channel_strategy: Box<ChannelStrategy<#output_type>> = Box::new(Forward::new(channel));
                        }
                    }
                };

                let verify = verify_and_start(&node_name, "system");

                quote! {
                    let actor_ref: ActorRefStrong<ArconMessage<#output_type>> = #target.actor_ref().hold().expect("failed to fetch actor ref");
                    let channel = Channel::Local(actor_ref);
                    #channel_strategy_quote
                    let (#node_name, reg) = system.create_and_register(move || {
                        Node::<#input_type, #output_type>::new(
                            #node_id.into(),
                            vec!(#predecessor.into()),
                            channel_strategy,
                            Box::new(#task_signature),
                            Box::new(InMemory::new("ignored for InMemory").unwrap()) // TODO: make customizable
                        )
                    });

                    #verify
                }
            }
            Some(ChannelKind::Remote(_)) => {
                unimplemented!();
            }
            None => panic!("Bad input"),
        }
    } else {
        unimplemented!();
        // Handle multiple channels..
    }
}

fn function_gen(spec_id: &str, name: &str, code: String) -> Ident {
    let mut func_map = GENERATED_FUNCTIONS.lock().unwrap();
    if let Some(map) = func_map.get_mut(spec_id) {
        if !map.contains_key(name) {
            map.insert(String::from(name), code);
        }
    }
    Ident::new(name, Span::call_site())
}

#[cfg(test)]
mod tests {}
