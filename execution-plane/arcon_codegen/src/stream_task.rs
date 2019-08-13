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
) -> TokenStream {
    let node_name = Ident::new(&node_name, Span::call_site());
    let input_type = to_token_stream(&task.input_type);
    let output_type = to_token_stream(&task.output_type);

    let task_ident_str = match &task.kind {
        FlatMap => "FlatMap",
        Map => "Map",
        Filter => "Filter",
    };

    let task_ident = Ident::new(task_ident_str, Span::call_site());
    let weld_code: &str = &task.weld_code;

    let successors: &Vec<spec::ChannelKind> = &task.successors;

    if *parallelism == 1 {
        assert_eq!(successors.len(), 1);

        match &successors.get(0).unwrap() {
            Local { id } => {
                let target = Ident::new(&id, Span::call_site());
                // Yeah... Fix this..
                let channel_strategy = {
                    match &task.kind {
                        FlatMap => {
                            quote! {
                                let channel_strategy: Box<ChannelStrategy<#output_type, FlatMap<#input_type, #output_type>>> = Box::new(Forward::new(channel));
                            }
                        }
                        Map => {
                            quote! {
                                let channel_strategy: Box<ChannelStrategy<#output_type, Map<#input_type, #output_type>>> = Box::new(Forward::new(channel));
                            }
                        }
                        Filter => {
                            quote! {
                                let channel_strategy: Box<ChannelStrategy<#output_type, Filter<#input_type>>> = Box::new(Forward::new(channel));
                            }
                        }
                    }
                };
                quote! {
                    let channel = Channel::Local(#target.actor_ref());
                    #channel_strategy

                    let code = String::from(#weld_code);
                    let module = std::sync::Arc::new(Module::new(code).unwrap());
                    let #node_name = system.create_and_start(move || #task_ident::new(module, Vec::new(), channel_strategy));
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
