use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};
use spec::SinkKind;
use spec::Type;

pub fn sink(name: &str, input_type: &Type, sink_type: &SinkKind) -> TokenStream {
    let sink_name = Ident::new(&name, Span::call_site());
    let input_type = to_token_stream(input_type);

    let sink_stream = match sink_type {
        SinkKind::Debug => debug_sink(&sink_name, &input_type),
        SinkKind::LocalFile { path } => local_file_sink(&sink_name, &input_type, &path),
        SinkKind::Socket { host: _, port: _ } => unimplemented!(),
    };

    sink_stream
}

fn debug_sink(sink_name: &Ident, input_type: &TokenStream) -> TokenStream {
    quote! {
        let #sink_name = system.create_and_start(move || {
            let sink: DebugSink<#input_type> = DebugSink::new();
            sink
        });
    }
}

fn local_file_sink(sink_name: &Ident, input_type: &TokenStream, file_path: &str) -> TokenStream {
    quote! {
        let #sink_name = system.create_and_start(move || {
            let sink: LocalFileSink<#input_type> = LocalFileSink::new(#file_path);
            sink
        });
    }
}
