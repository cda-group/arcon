use crate::common::generate_function;
use crate::common::verify_and_start;
use crate::spec::channel_kind::ChannelKind;
use crate::spec::window;
use crate::spec::Window;
use crate::types::to_token_stream;
use proc_macro2::{Ident, Span, TokenStream};

pub fn window(id: u32, window: &Window, spec_id: &String) -> TokenStream {
    let name = "node".to_string() + &id.to_string();
    let input_type = to_token_stream(&window.input_type.clone().unwrap(), spec_id);
    let output_type = to_token_stream(&window.output_type.clone().unwrap(), spec_id);
    let name = Ident::new(&name, Span::call_site());

    let successors = &window.successors;
    let predecessor = window.predecessor;

    // NOTE: We just support 1 output channel for now
    assert_eq!(successors.len(), 1);

    let successor = match successors.get(0).unwrap().channel_kind.as_ref() {
        Some(ChannelKind::LocalChannel(_)) => id,
        Some(ChannelKind::RemoteChannel(_)) => {
            unimplemented!();
        }
        None => panic!("non supported channel"),
    };

    let successor_ident = Ident::new(&successor.to_string(), Span::call_site());

    let window_stream = match window.assigner.as_ref() {
        Some(window::Assigner::Sliding(_)) => unimplemented!(),
        Some(window::Assigner::Tumbling(t)) => {
            let window_code = window_function(&window.function.clone().unwrap());
            let window_comp = tumbling(
                &name,
                &window.time_kind.as_ref().unwrap(),
                window.keyed,
                t.length,
                &successor_ident,
                &input_type,
                &output_type,
                id,
                predecessor,
            );
            crate::combine_token_streams(window_code, window_comp)
        }
        None => panic!("bad input"),
    };

    window_stream
}

fn window_function(window_function: &crate::spec::window::Function) -> TokenStream {
    let code = match window_function {
        window::Function::AppenderWindow(aw) => crate::common::generate_function(&aw.udf),
        window::Function::IncrementalWindow(iw) => {
            let udf = crate::common::generate_function(&iw.udf);
            let agg = crate::common::generate_function(&iw.agg_udf);
            crate::combine_token_streams(udf, agg)
        }
    };
    code
}

fn tumbling(
    name: &Ident,
    time_kind: &window::TimeKind,
    keyed: bool,
    window_len: u64,
    successor: &Ident,
    input_type: &TokenStream,
    output_type: &TokenStream,
    node_id: u32,
    predecessor: u32,
) -> TokenStream {
    let keyed = quote! { #keyed };

    let component = match time_kind {
        window::TimeKind::EventTime(e) => {
            let slack = Ident::new(&e.slack.to_string(), Span::call_site());
            quote! {
             EventTimeWindowAssigner::<#input_type, #output_type>::new(
                builder_code,
                udf_code,
                materialiser_code,
                #window_len,
                #window_len,
                #slack,
                #keyed,
            )
            }
        }
        window::TimeKind::ProcessingTime(_) => {
            quote! {
             ProcessingTimeWindowAssigner::<#input_type, #output_type>::new(
                builder_code,
                udf_code,
                materialiser_code,
                #window_len as u128,
                #window_len as u128,
                0 as u128,
                #keyed
            )
            }
        }
        window::TimeKind::IngestionTime(_) => {
            quote! {
             EventTimeWindowAssigner::<#input_type, #output_type>::new(
                builder_code,
                udf_code,
                materialiser_code,
                #window_len,
                #window_len,
                0,
                #keyed
            )
            }
        }
    };

    let verify = verify_and_start(name, "system");

    quote! {
        let actor_ref: ActorRef<ArconMessage<#output_type>> = #successor.actor_ref();
        let channel = Channel::Local(actor_ref);
        let channel_strategy: Box<Forward<#output_type>> =
            Box::new(Forward::new(channel));

        let (#name, reg) = system.create_and_register(move || {
            Node::<#input_type, #output_type>::new(
                #node_id.into(),
                vec!(#predecessor.into()),
                channel_strategy,
                Box::new(#component)
            )
        });
        #verify
    }
}
