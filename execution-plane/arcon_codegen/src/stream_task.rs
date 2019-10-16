use crate::common::id_to_ident;
use crate::common::verify_and_start;
use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};
use spec::ChannelKind::*;
use spec::Task;
use spec::TaskKind::{Filter, FlatMap, Map};

pub fn stream_task(
    id: u32,
    _target_name: &str,
    parallelism: &u32,
    task: &Task,
    spec_id: &String,
) -> TokenStream {
    let node_id = id;
    let node_name = id_to_ident(id);
    let input_type = to_token_stream(&task.input_type, spec_id);
    let output_type = to_token_stream(&task.output_type, spec_id);

    let weld_code: &str = &task.weld_code;

    let successors: &Vec<spec::ChannelKind> = &task.successors;
    let predecessor = &task.predecessor;

    if *parallelism == 1 {
        assert_eq!(successors.len(), 1);

        match &successors.get(0).unwrap() {
            Local { id } => {
                let target = Ident::new(&id, Span::call_site());
                let task_signature = {
                    match &task.kind {
                        FlatMap => {
                            quote! {
                                FlatMap::<#input_type, #output_type>::new(module)
                            }
                        }
                        Map => {
                            quote! {
                                Map::<#input_type, #output_type>::new(module)
                            }
                        }
                        Filter => {
                            quote! {
                                Filter::<#input_type>::new(module)
                            }
                        }
                    }
                };

                let channel_strategy_quote = match &task.kind {
                    Filter => {
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
                    let code = String::from(#weld_code);
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
            Remote { id: _, addr: _ } => {
                unimplemented!();
            }
        }
    } else {
        unimplemented!();
        // Handle multiple channels..
    }
}

#[cfg(test)]
mod tests {}
