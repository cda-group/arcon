use proc_macro2::{Ident, Span, TokenStream};

pub fn task(name: &str) -> TokenStream {
    let task_name = Ident::new(&name, Span::call_site());

    let imports = quote! {
        extern crate codegen;
        use codegen::prelude::*;
    };

    let struct_def = quote! {
        #[derive(ComponentDefinition)]
        pub struct #task_name {
            ctx: ComponentContext<#task_name>,
            udf: Module,
            udf_avg: u64,
            udf_executions: u64,
            id: String,
        }
    };

    let task_impl = quote! {
        impl #task_name {
            pub fn new(
                id: String,
                udf: Module,
            ) -> #task_name {
                #task_name {
                    ctx: ComponentContext::new(),
                    udf,
                    udf_avg: 0,
                    udf_executions: 0,
                    id,
                }
            }

        }
    };

    let control_port = task_control_port(&name);
    let actor_impl = actor_impl(&name, None, Some(recv_msg_stream()));

    quote! {
        #imports #struct_def #task_impl #control_port #actor_impl
    }
}

fn actor_impl(
    task_name: &str,
    recv_local: Option<TokenStream>,
    recv_msg: Option<TokenStream>,
) -> TokenStream {
    let task_name = Ident::new(&task_name, Span::call_site());

    quote! {
        impl Actor for #task_name {
            fn receive_local(&mut self, sender: ActorRef, msg: Box<Any>) {
                #recv_local
            }
            fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
                #recv_msg
            }
        }
    }
}

fn recv_msg_stream() -> TokenStream {
    quote! {
        unimplemented!();
    }
}

fn task_control_port(task_name: &str) -> TokenStream {
    let task_name = Ident::new(&task_name, Span::call_site());

    quote! {
        impl Provide<ControlPort> for #task_name {
            fn handle(&mut self, event: ControlEvent) -> () {
                match event {
                    ControlEvent::Start => {
                        unimplemented!();
                    }
                    ControlEvent::Stop => {
                        unimplemented!();

                    }
                    ControlEvent::Kill => {
                        unimplemented!();
                    }
                }
            }
        }
    }
}
