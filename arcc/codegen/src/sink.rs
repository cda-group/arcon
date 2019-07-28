use proc_macro2::{Ident, Span, TokenStream};
use spec::SinkKind;

pub fn sink(name: &str, input_type: &str, sink_type: &SinkKind) -> TokenStream {
    let sink_name = Ident::new(&name, Span::call_site());
    let input_type = Ident::new(&input_type, Span::call_site());

    let sink_stream = match sink_type {
        SinkKind::Debug => debug_sink(&sink_name, &input_type),
        SinkKind::Socket { host: _, port: _ } => unimplemented!(),
    };

    sink_stream
}

fn debug_sink(sink_name: &Ident, input_type: &Ident) -> TokenStream {
    quote! {
        let #sink_name = system.create_and_start(move || {
            let sink: DebugSink<#input_type> = DebugSink::new();
            sink
        });
    }
}
