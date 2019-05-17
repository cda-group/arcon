/// Experimental code generation
use proc_macro2::{Ident, Span, TokenStream};

pub fn task(name: &str) -> TokenStream {
    let task_name = Ident::new(&name, Span::call_site());

    let struct_def = quote! {
        #[derive(ComponentDefinition)]
        pub struct #task_name {
            ctx: ComponentContext<#task_name>,
            report_timer: Option<ScheduledTimer>,
            manager_port: RequiredPort<MetricPort, Task>,
            output_path: ActorPath,
            udf: Module,
            udf_avg: u64,
            udf_executions: u64,
            backend: Arc<StateBackend>,
            id: String,
        }
    };

    let task_impl = quote! {
        impl #task_name {
            pub fn new(
                id: String,
                udf: Module,
                backend: Arc<StateBackend>,
                output_path: ActorPath,
            ) -> #task_name {
                #task_name {
                    ctx: ComponentContext::new(),
                    report_timer: None,
                    manager_port: RequiredPort::new(),
                    output_path,
                    udf,
                    udf_avg: 0,
                    udf_executions: 0,
                    backend: Arc::clone(&backend),
                    id,
                }
            }

            fn stop_report(&mut self) {
                if let Some(timer) = self.report_timer.clone() {
                    self.cancel_timer(timer);
                    self.report_timer = None;
                }
            }
            fn update_avg(&mut self, ns: u64) {
                if self.udf_executions == 0 {
                    self.udf_avg = ns;
                } else {
                    let ema: u64 = (ns - self.udf_avg) * (2 / (self.udf_executions + 1)) + self.udf_avg;
                    self.udf_avg = ema;
                }
                self.udf_executions += 1;
            }
        }
    };

    let control_port = task_control_port(&name, 200);
    let actor_impl = actor_impl(&name, None, Some(recv_msg_stream()));

    quote! {
        #struct_def #task_impl #control_port #actor_impl
    }
}

fn actor_impl(
    task_name: &str,
    recv_local: Option<TokenStream>,
    recv_msg: Option<TokenStream>,
) -> TokenStream {
    let task_name = Ident::new(&task_name, Span::call_site());

    let task_actor = quote! {
        impl Actor for #task_name {
            fn receive_local(&mut self, sender: ActorRef, msg: Box<Any>) {
                #recv_local
            }
            fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
                #recv_msg
            }
        }
    };

    task_actor
}

fn recv_msg_stream() -> TokenStream {
    quote! {
        let r: Result<TaskMsg, SerError> = core::messages::ProtoSer::deserialise(buf);
        if let Ok(msg) = r {
            match msg.payload.unwrap() {
                TaskMsg_oneof_payload::watermark(_) => {}
                TaskMsg_oneof_payload::element(e) => {
                    if let Err(err) = self.run_udf(e) {
                        error!(
                            self.ctx.log(),
                            "Failed to run Task UDF with err: {}",
                            err.to_string()
                        );
                    }
                }
                TaskMsg_oneof_payload::checkpoint(_) => {
                    let _ = self.backend.checkpoint("some_id".to_string());
                }
            }
        }
        else {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }
}

fn task_control_port(task_name: &str, timer_ident: u64) -> proc_macro2::TokenStream {
    let task_name = Ident::new(&task_name, Span::call_site());

    let control_port = quote! {
        impl Provide<ControlPort> for #task_name {
            fn handle(&mut self, event: ControlEvent) -> () {
                match event {
                    ControlEvent::Start => {
                        let timeout = std::time::Duration::from_millis(#timer_ident);
                        let timer = self.schedule_periodic(timeout, timeout, |self_c, _| {
                            self_c.manager_port.trigger(Metric {
                                task_id: self_c.id.clone(),
                                task_avg: self_c.udf_avg,
                            });
                        });

                        self.report_timer = Some(timer);
                    }
                    ControlEvent::Stop => self.stop_report(),
                    ControlEvent::Kill => self.stop_report(),
                }
            }
        }
    };

    control_port
}

#[cfg(test)]
mod tests {

}
