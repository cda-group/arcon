use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};
use spec::{ChannelKind, Window, WindowAssigner, WindowFunction, TimeKind, WindowKind, WindowKind::Keyed, WindowKind::All};

pub fn window(name: &str, window: &Window) -> TokenStream {
    let input_type = to_token_stream(&window.window_function.input_type);
    let output_type = to_token_stream(&window.window_function.output_type);
    let builder_type = to_token_stream(&window.window_function.builder_type);
    let name = Ident::new(&name, Span::call_site());

    let successors = &window.successors;

    // NOTE: We just support 1 output channel for now
    assert_eq!(successors.len(), 1);
    let successor = match successors.get(0).unwrap() {
        ChannelKind::Local { id } => id,
        ChannelKind::Remote { id: _, addr: _ } => {
            unimplemented!();
        }
    };

    let successor_ident = Ident::new(&successor, Span::call_site());

    let window_stream = match window.assigner {
        WindowAssigner::Tumbling { length } => {
            let window_code = window_modules(&window.window_function);
            let window_comp = tumbling(
                &name,
                &window.time_kind,
                &window.window_kind,
                length,
                &successor_ident,
                &input_type,
                &output_type,
                &builder_type,
            );
            crate::combine_token_streams(window_code, window_comp)
        }
        WindowAssigner::Sliding {
            length: _,
            slide: _,
        } => {
            unimplemented!();
        }
    };

    window_stream
}

fn window_modules(window_function: &WindowFunction) -> TokenStream {
    let builder_code: &str = &window_function.builder;
    let udf_code: &str = &window_function.udf;
    let materialiser_code: &str = &window_function.materialiser;
    quote! {
        let builder_code = String::from(#builder_code);
        let udf_code = String::from(#udf_code);
        let materialiser_code = String::from(#materialiser_code);
    }
}


fn tumbling(
    name: &Ident,
    time_kind: &TimeKind,
    window_kind: &WindowKind,
    window_len: u64,
    successor: &Ident,
    input_type: &TokenStream,
    output_type: &TokenStream,
    builder_type: &TokenStream,
) -> TokenStream {
    let keyed = match window_kind {
        Keyed { kind } => unimplemented!(),
        All => quote! { false }
    };

    let component = match time_kind {
        TimeKind::Event { slack } => {
            quote! {
             EventTimeWindowAssigner::<#input_type, #builder_type, #output_type>::new(
                channel_strategy,
                builder_code,
                udf_code,
                materialiser_code,
                #window_len,
                #window_len,
                #slack,
                #keyed,
            )
            }
        },
        TimeKind::Processing => {
            quote! {
             ProcessingTimeWindowAssigner::<#input_type, #builder_type, #output_type>::new(
                channel_strategy,
                builder_code,
                udf_code,
                materialiser_code,
                #window_len as u128,
                #window_len as u128,
                0 as u128,
                #keyed
            )
            }
        },
        TimeKind::Ingestion => {
            quote! {
             EventTimeWindowAssigner::<#input_type, #builder_type, #output_type>::new(
                channel_strategy,
                builder_code,
                udf_code,
                materialiser_code,
                #window_len,
                #window_len,
                0,
                #keyed
            )
            }
        },
    };


    quote! {
        let channel_strategy: Box<Forward<#output_type>> =
            Box::new(Forward::new(Channel::Local(#successor.actor_ref())));

        let #name = system.create_and_start(move || {
            #component
        });
    }
}
