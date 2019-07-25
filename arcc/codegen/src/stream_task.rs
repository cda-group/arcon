use proc_macro2::{Ident, Span, TokenStream};

pub fn stream_task(
    node_name: &str,
    target_name: &str,
    weld_code: &str,
    input_type: &str,
    output_type: &str,
) -> TokenStream {
    let node_name = Ident::new(&node_name, Span::call_site());
    let input_type = Ident::new(&input_type, Span::call_site());
    let output_type = Ident::new(&output_type, Span::call_site());
    let target_name = Ident::new(&target_name, Span::call_site());

    quote! {
        let target_port = #target_name.on_definition(|c| c.in_port.share());
        let mut req_port: RequiredPort<
            ChannelPort<#output_type>,
            StreamTask<#input_type, ChannelPort<#output_type>, #output_type>,
        > = RequiredPort::new();
        let _ = req_port.connect(target_port);

        let ref_port = RequirePortRef(std::rc::Rc::new(std::cell::UnsafeCell::new(req_port)));
        let channel = Channel::Port(ref_port);
        let channel_strategy = Box::new(Forward::new(channel));

        let code = String::from(#weld_code);
        let module = std::sync::Arc::new(Module::new(code).unwrap());
        let #node_name = system.create_and_start(move || StreamTask::new(module, Vec::new(), channel_strategy));
    }
}

#[cfg(test)]
mod tests {

}
