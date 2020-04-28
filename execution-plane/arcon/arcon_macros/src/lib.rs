// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! The arcon_macros crate contains macros used by [arcon].

#![recursion_limit = "128"]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::{parse_macro_input, DeriveInput};

const VERSION_ID: &str = "version";
const UNSAFE_SER_ID: &str = "unsafe_ser_id";
const RELIABLE_SER_ID: &str = "reliable_ser_id";
const KEYS: &str = "keys";

/// arcon is a proc macro for defining an ArconType struct
///
/// #[arcon(reliable_ser_id = 1, unsafe_ser_id = 2, version = 1)]
/// or
/// #[arcon(reliable_ser_id = 1, unsafe_ser_id = 2, version = 1, keys = id)]
#[proc_macro_attribute]
pub fn arcon(args: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;
    let arcon_args: Vec<(String, String)> = args
        .to_string()
        .trim()
        .split(',')
        .map(|s| {
            let a: Vec<String> = s
                .trim()
                .to_string()
                .split(" ")
                .map(|s| s.to_string())
                .collect();
            assert_eq!(a.len(), 3, "Expecting the following arg setup: unsafe_ser_id = 1, reliable_ser_id = 2, version = 1");
            assert_eq!(a[1], "=");
            (a[0].to_string(), a[2].to_string())
        })
        .filter(|(arg_name, _)| {
            arg_name == VERSION_ID
                || arg_name == UNSAFE_SER_ID
                || arg_name == RELIABLE_SER_ID
                || arg_name == KEYS
        })
        .collect();

    assert!(
        arcon_args.len() >= 3 && arcon_args.len() <= 4,
        "arcon requires at least 3 args: unsafe_ser_id, reliable_ser_id, version"
    );

    let raw_ids: Vec<u64> = arcon_args
        .iter()
        .filter(|(arg_name, _)| arg_name != KEYS && arg_name != VERSION_ID)
        .map(|(_, arg_value)| arg_value.parse::<u64>().unwrap())
        .collect();

    assert_eq!(raw_ids.len(), 2);
    assert_ne!(raw_ids[0], raw_ids[1], "UNSAFE_SER_ID and RELIABLE_SER_ID must have different values");

    let ids: Vec<proc_macro2::TokenStream> = arcon_args
        .iter()
        .filter(|(arg_name, _)| arg_name != KEYS)
        .map(|(arg_name, arg_value)| {
            if arg_name == RELIABLE_SER_ID {
                let value = arg_value.parse::<u64>().unwrap();
                quote! { const RELIABLE_SER_ID: SerId  = #value; }
            } else if arg_name == UNSAFE_SER_ID {
                let value = arg_value.parse::<u64>().unwrap();
                quote! { const UNSAFE_SER_ID: SerId = #value; }
            } else {
                let value = arg_value.parse::<u32>().unwrap();
                quote! { const VERSION_ID: VersionId = #value; }
            }
        })
        .collect();

    let keys: Option<String> = arcon_args
        .iter()
        .find(|(arg_name, _)| arg_name == KEYS)
        .map(|(_, value)| value.to_owned());

    if let syn::Data::Struct(ref s) = item.data {
        let generics = &item.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        // If keys are specified "keys = ..", we hash on just those fields.
        // Otherwise, the struct will be hashed using all fields..
        let field_hashes = {
            if let Some(keys) = keys {
                let keys: Vec<proc_macro2::TokenStream> = keys
                    .trim()
                    .split(',')
                    .map(|k| {
                        let struct_field = Ident::new(&k.trim(), Span::call_site());
                        quote! { self.#struct_field.hash(state); }
                    })
                    .collect();
                keys
            } else {
                let mut field_hashes = Vec::new();
                for field in s.fields.iter() {
                    match field.ident {
                        Some(ref ident) => {
                            let field_hash = quote! { self.#ident.hash(state); };
                            field_hashes.push(field_hash);
                        }
                        None => panic!("Struct missing identiy"),
                    }
                }
                field_hashes
            }
        };

        #[allow(unused)]
        let maybe_serde = quote! {};
        #[cfg(feature = "arcon_serde")]
        let maybe_serde = quote! {
            #[derive(::serde::Serialize, ::serde::Deserialize)]
        };

        let output: proc_macro2::TokenStream = {
            quote! {
                #maybe_serde
                #[derive(Clone, ::abomonation_derive::Abomonation, ::prost::Message)]
                #item
                impl #impl_generics ArconType for #name #ty_generics #where_clause {
                    #(#ids)*
                }
                impl ::std::hash::Hash for #name {
                    fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
                        #(#field_hashes)*
                    }
                }
            }
        };

        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[arcon] is only defined for structs!");
    }
}

/// Implements [std::str::FromStr] for a struct using a delimiter
///
/// If no delimiter is specified, then `,` is chosen as default.
/// Note: All inner fields of the struct need to implement [std::str::FromStr] for the macro to work.
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
        for field in s.fields.iter() {
            match field.ident {
                Some(ref ident) => idents.push((ident.clone(), &field.ty)),
                None => panic!("Struct missing identiy"),
            }
        }

        let mut field_quotes = Vec::new();
        for (pos, (ident, ty)) in idents.iter().enumerate() {
            let parse = quote! {
                string_vec[#pos].parse::<#ty>().map_err(|_| String::from("Failed to parse field"))?
            };
            let field_gen = quote! { #ident: #parse };
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
