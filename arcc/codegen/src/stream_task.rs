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
    let input_type = Ident::new(&task.input_type, Span::call_site());
    let output_type = Ident::new(&task.output_type, Span::call_site());

    let task_ident_str = match &task.kind {
        FlatMap => "FlatMap",
        Map => "Map",
        Filter => "Filter",
    };

    let task_ident = Ident::new(task_ident_str, Span::call_site());
    let weld_code: &str = &task.weld_code;

    let channels: &Vec<spec::ChannelKind> = &task.channels;

    if *parallelism == 1 {
        assert_eq!(channels.len(), 1);

        match &channels.get(0).unwrap() {
            Local { id } => {
                let target_name = Ident::new(&id, Span::call_site());
                quote! {
                    let target_port = #target_name.on_definition(|c| c.in_port.share());
                    let mut req_port: RequiredPort<
                        ChannelPort<#output_type>,
                        #task_ident<#input_type, ChannelPort<#output_type>, #output_type>,
                    > = RequiredPort::new();
                    let _ = req_port.connect(target_port);

                    let ref_port = RequirePortRef(std::rc::Rc::new(std::cell::UnsafeCell::new(req_port)));
                    let channel = Channel::Port(ref_port);
                    let channel_strategy = Box::new(Forward::new(channel));

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
