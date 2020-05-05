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
use quote::ToTokens;
use syn::{parse_macro_input, DeriveInput};

/// Derive macro for defining an Arcon supported struct
///
/// ```rust,ignore
/// #[derive(Arcon)]
/// #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1)]
/// struct ArconStruct {}
/// ```
#[proc_macro_derive(Arcon, attributes(arcon))]
pub fn arcon(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let name = &input.ident;

    let meta = input
        .attrs
        .iter()
        .find_map(|attr| match attr.parse_meta() {
            Ok(m) => {
                if m.path().is_ident("arcon") {
                    Some(m)
                } else {
                    None
                }
            }
            Err(e) => panic!("unable to parse attribute: {}", e),
        })
        .expect("no attribute 'arcon' found");

    let meta_list = match meta {
        syn::Meta::List(inner) => inner,
        _ => panic!("attribute 'arcon' has incorrect type"),
    };

    let mut unsafe_ser_id = None;
    let mut reliable_ser_id = None;
    let mut version = None;
    let mut keys = None;

    for item in meta_list.nested {
        let pair = match item {
            syn::NestedMeta::Meta(syn::Meta::NameValue(ref pair)) => pair,
            _ => panic!(
                "unsupported attribute argument {:?}",
                item.to_token_stream()
            ),
        };

        if pair.path.is_ident("unsafe_ser_id") {
            if let syn::Lit::Int(ref s) = pair.lit {
                unsafe_ser_id = Some(s.base10_parse::<u64>().unwrap());
            } else {
                panic!("unsafe_ser_id must be an Int literal");
            }
        } else if pair.path.is_ident("reliable_ser_id") {
            if let syn::Lit::Int(ref s) = pair.lit {
                reliable_ser_id = Some(s.base10_parse::<u64>().unwrap());
            } else {
                panic!("reliable_ser_id must be an Int literal");
            }
        } else if pair.path.is_ident("version") {
            if let syn::Lit::Int(ref s) = pair.lit {
                version = Some(s.base10_parse::<u32>().unwrap());
            } else {
                panic!("version must be an Int literal");
            }
        } else if pair.path.is_ident("keys") {
            if let syn::Lit::Str(ref s) = pair.lit {
                keys = Some(s.value())
            } else {
                panic!("keys must be string literal");
            }
        } else {
            panic!(
                "unsupported attribute key '{}' found",
                pair.path.to_token_stream()
            )
        }
    }

    if unsafe_ser_id.is_none() || reliable_ser_id.is_none() || version.is_none() {
        panic!("arcon attr expects unsafe_ser_id, reliable_ser_id, version args");
    }

    let unsafe_ser_id = unsafe_ser_id.unwrap();
    let reliable_ser_id = reliable_ser_id.unwrap();
    let version = version.unwrap();

    // Id check
    assert_ne!(
        unsafe_ser_id, reliable_ser_id,
        "UNSAFE_SER_ID and RELIABLE_SER_ID must have different values"
    );

    let mut ids: Vec<proc_macro2::TokenStream> = Vec::with_capacity(3);
    ids.push(quote! { const RELIABLE_SER_ID: SerId  = #reliable_ser_id; });
    ids.push(quote! { const UNSAFE_SER_ID: SerId = #unsafe_ser_id; });
    ids.push(quote! { const VERSION_ID: VersionId = #version; });

    if let syn::Data::Struct(ref s) = input.data {
        let generics = &input.generics;
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

        let output: proc_macro2::TokenStream = {
            quote! {
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
        panic!("#[derive(Arcon)] can only be derived for Structs");
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
