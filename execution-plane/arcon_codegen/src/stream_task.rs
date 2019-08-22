use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};
use spec::ChannelKind::*;
use spec::Task;
use spec::TaskKind::{Filter, FlatMap, Map};

pub fn stream_task(
    node_name: &str,
    _target_name: &str,
    parallelism: &u32,
    task: &Task,
    spec_id: &String,
) -> TokenStream {
    let node_name = Ident::new(&node_name, Span::call_site());
    let input_type = to_token_stream(&task.input_type, spec_id);
    let output_type = to_token_stream(&task.output_type, spec_id);

    let weld_code: &str = &task.weld_code;

    let successors: &Vec<spec::ChannelKind> = &task.successors;

    if *parallelism == 1 {
        assert_eq!(successors.len(), 1);

        match &successors.get(0).unwrap() {
            Local { id } => {
                let target = Ident::new(&id, Span::call_site());
                let task_signature = {
                    match &task.kind {
                        FlatMap => {
                            quote! {
                                FlatMap::<#input_type, #output_type>::new(module, Vec::new(), channel_strategy)
                            }
                        }
                        Map => {
                            quote! {
                                Map::<#input_type, #output_type>::new(module, Vec::new(), channel_strategy)
                            }
                        }
                        Filter => {
                            quote! {
                                Filter::<#input_type>::new(module, Vec::new(), channel_strategy)
                            }
                        }
                    }
                };

                let channel_strategy_quote = match &task.kind {
                    Filter => {
                        quote! {
                        let channel_strategy: Box<ChannelStrategy<#input_type>> = Box::new(Forward::new(channel));
                    }
                    },
                    _ => {
                        quote! {
                        let channel_strategy: Box<ChannelStrategy<#output_type>> = Box::new(Forward::new(channel));
                    }
                    }
                };

                quote! {
                    let channel = Channel::Local(#target.actor_ref());
                    #channel_strategy_quote
                    let code = String::from(#weld_code);
                    let module = std::sync::Arc::new(Module::new(code).unwrap());
                    let (#node_name, reg) = system.create_and_register(move || #task_signature);

                    reg.wait_timeout(std::time::Duration::from_millis(1000))
                        .expect("Component never registered!")
                        .expect("Component failed to register!");
                    system.start(&#node_name);
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
