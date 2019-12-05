use crate::common::id_to_ident;
use crate::common::verify_and_start;
use crate::spec;
use crate::spec::channel_kind::ChannelKind;
use crate::types::to_token_stream;
use arcon_proto::arcon_spec::{Function, FunctionKind};
use proc_macro2::{Ident, Span, TokenStream};

pub fn function(
    id: u32,
    _target_name: &str,
    parallelism: &u32,
    func: &Function,
    spec_id: &String,
) -> TokenStream {
    let node_id = id;
    let node_name = id_to_ident(id);
    let input_type = to_token_stream(&func.input_type.clone().unwrap(), spec_id);
    let output_type = to_token_stream(&func.output_type.clone().unwrap(), spec_id);

    let successors: &Vec<spec::ChannelKind> = &func.successors;
    let predecessor = func.predecessor;

    if *parallelism == 1 {
        assert_eq!(successors.len(), 1);

        match &successors.get(0).unwrap().channel_kind.as_ref() {
            Some(ChannelKind::LocalChannel(_)) => {
                let target = Ident::new(&id.to_string(), Span::call_site());
                let kind: FunctionKind = unsafe { ::std::mem::transmute(func.kind) };
                let task_signature = {
                    match &kind {
                        FunctionKind::FlatMap => {
                            quote! {
                                FlatMap::<#input_type, #output_type>::new(module)
                            }
                        }
                        FunctionKind::Map => {
                            quote! {
                                Map::<#input_type, #output_type>::new(module)
                            }
                        }
                        FunctionKind::Filter => {
                            quote! {
                                Filter::<#input_type>::new(module)
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
                    let actor_ref: ActorRef<ArconMessage<#output_type>> = #target.actor_ref();
                    let channel = Channel::Local(actor_ref);
                    #channel_strategy_quote
                    let code = String::from(#);
                    let module = std::sync::Arc::new(Module::new(code).unwrap());
                    let (#node_name, reg) = system.create_and_register(move || {
                        Node::<#input_type, #output_type>::new(
                            #node_id.into(),
                            vec!(#predecessor.into()),
                            channel_strategy,
                            Box::new(#task_signature)
                        )
                    });

                    #verify
                }
            }
            Some(ChannelKind::RemoteChannel(_)) => {
                unimplemented!();
            }
            None => panic!("Bad input"),
        }
    } else {
        unimplemented!();
        // Handle multiple channels..
    }
}

#[cfg(test)]
mod tests {}
