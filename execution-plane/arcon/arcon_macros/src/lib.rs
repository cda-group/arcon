#![recursion_limit = "128"]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_attribute]
pub fn arcon(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;
    let _ = proc_macro2::TokenStream::from(metadata);

    if let syn::Data::Struct(_) = item.data {
        let generics = &item.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let output: proc_macro2::TokenStream = {
            quote! {
                #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
                #[repr(C)]
                #item
                impl #impl_generics ArconType for #name #ty_generics #where_clause {}
            }
        };

        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[arcon] is only defined for structs!");
    }
}

/// arcon_task macro used for unified event passing between components
/// Adds all necessary methods needed to declare a kompact component.
/// Requires the generic attributes IN and PORT to be declared where:
///     IN is the ArconType which the component will receive
/// Requires the component to implement the functions handle_element and handle_watermark
#[proc_macro_attribute]
pub fn arcon_task(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;
    let _ = proc_macro2::TokenStream::from(metadata);

    if let syn::Data::Struct(_) = item.data {
        let generics = &item.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let output: proc_macro2::TokenStream = {
            quote! {
                use crate::messages::protobuf::messages::StreamTaskMessage;
                use crate::messages::protobuf::ProtoSer;
                #item
                impl #impl_generics Provide<ControlPort> for #name #ty_generics #where_clause {
                    fn handle(&mut self, _event: ControlEvent) -> () {}
                }

                impl #impl_generics Actor for #name #ty_generics #where_clause {
                    fn receive_local(&mut self, sender: ActorRef, msg: &Any) {
                        if let Some(event) = msg.downcast_ref::<ArconEvent<IN>>() {
                            let _ = self.handle_event(event);
                        }
                    }
                    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
                        if ser_id == serialisation_ids::PBUF {
                            let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
                            if let Ok(msg) = r {
                                if let Ok(event) = ArconEvent::from_remote(msg) {
                                    if let Err(err) = self.handle_event(&event) {
                                        error!(self.ctx.log(), "Unable to handle event, error {}", err);
                                    }
                                } else {
                                    error!(self.ctx.log(), "Failed to convert remote message to local");
                                }
                            } else {
                                error!(self.ctx.log(), "Failed to deserialise StreamTaskMessage",);
                            }
                        } else {
                            error!(self.ctx.log(), "Got unexpected message from {}", sender);
                        }
                    }
                }

                unsafe impl #impl_generics Send for #name #ty_generics #where_clause {
                }

                unsafe impl #impl_generics Sync for #name #ty_generics #where_clause {
                }

                impl #impl_generics #name #ty_generics #where_clause {
                    fn handle_event(&mut self, event: &ArconEvent<IN>) -> ArconResult<()> {
                        match event {
                            ArconEvent::Element(e) => {
                                self.handle_element(e)?;
                            }
                            ArconEvent::Watermark(w) => {
                                self.handle_watermark(*w)?;
                            }
                        }
                        Ok(())
                    }
                }
            }
        };
        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[arcon_task] is only defined for structs!");
    }
}

/// `arcon_decoder` generates a FromStr impl for its struct
///
///
/// NOTE: the inner fields need to implement FromStr as well
#[proc_macro_attribute]
pub fn arcon_decoder(delimiter: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;

    if let syn::Data::Struct(ref s) = item.data {
        let delim_str: String = delimiter.to_string();
        // Set comma to default if no other is specified..
        let delim = if delim_str.is_empty() {
            ","
        } else {
            &delim_str
        };

        let mut idents = Vec::new();
        for ref field in s.fields.iter() {
            match &field.ident {
                &Some(ref ident) => idents.push((ident.clone(), &field.ty)),
                &None => panic!("Struct missing identiy"),
            }
        }

        let mut field_quotes = Vec::new();
        let mut pos: usize = 0;
        for (ident, ty) in idents.iter() {
            let parse = quote! { 
                string_vec[#pos].parse::<#ty>().map_err(|_| String::from("Failed to parse field"))?
            };
            let field_gen = quote! { #ident: #parse };
            pos += 1;
            field_quotes.push(field_gen);
        }

        let from_str = quote! {Self{#(#field_quotes,)*}};

        let output: proc_macro2::TokenStream = {
            quote! {
                #item
                impl ::std::str::FromStr for #name {
                    type Err = String;
                    fn from_str(s: &str) -> ::std::result::Result<Self, String> {
                        let string_vec: Vec<&str> = s.trim()
                            .split(#delim)
                            .collect::<Vec<&str>>()
                            .iter()
                            .map(|s| s.trim() as &str)
                            .collect();

                        Ok(#from_str)
                    }
                }
            }
        };

        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[arcon_decoder] is only defined for structs!");
    }
}
