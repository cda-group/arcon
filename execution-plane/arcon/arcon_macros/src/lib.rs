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

/// An estimation of how many bytes a string contains
const STRING_BYTES_ESTIMATION: usize = 10;
/// An estimation of how many bytes a byte array contains
const BYTE_ARRAY_ESTIMATION: usize = 10;

const STRING_IDENT: &str = "String";
const VEC_IDENT: &str = "Vec";
const U8_IDENT: &str = "u8";
const U32_IDENT: &str = "u32";
const U64_IDENT: &str = "u64";
const I32_IDENT: &str = "i32";
const I64_IDENT: &str = "i64";
const BOOL_IDENT: &str = "bool";

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

    let (unsafe_ser_id, reliable_ser_id, version, keys) = {
        let arcon_attr = input.attrs.iter().find_map(|attr| match attr.parse_meta() {
            Ok(m) => {
                if m.path().is_ident("arcon") {
                    Some(m)
                } else {
                    None
                }
            }
            Err(e) => panic!("unable to parse attribute: {}", e),
        });

        if let Some(meta) = arcon_attr {
            // If there exists a #[arcon(..)], then handle the meta list
            let meta_list = match meta {
                syn::Meta::List(inner) => inner,
                _ => panic!("attribute 'arcon' has incorrect type"),
            };

            arcon_attr_meta(meta_list)
        } else {
            // If no arcon attr is defined, then attempt to parse attrs from doc comments
            let mut doc_attrs = Vec::new();
            for i in input.attrs.iter() {
                let attr = i.parse_meta().expect("failed to parse meta");
                if attr.path().is_ident("doc") {
                    doc_attrs.push(attr.clone());
                }
            }
            arcon_doc_attr(doc_attrs)
        }
    };

    if unsafe_ser_id.is_none() {
        panic!("missing unsafe_ser_id attr");
    }

    if reliable_ser_id.is_none() {
        panic!("missing reliable_ser_id attr");
    }

    if version.is_none() {
        panic!("missing version attr");
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
    ids.push(quote! { const RELIABLE_SER_ID: ::arcon::SerId  = #reliable_ser_id; });
    ids.push(quote! { const UNSAFE_SER_ID: ::arcon::SerId = #unsafe_ser_id; });
    ids.push(quote! { const VERSION_ID: ::arcon::VersionId = #version; });

    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let key_quote: proc_macro2::TokenStream;

    if let syn::Data::Struct(ref s) = input.data {
        if let Some(keys) = keys {
            // keys were specified...
            let fields: Vec<String> = keys.trim().split(',').map(|s| s.to_string()).collect();
            let estimated_bytes = bytes_estimation(s, fields.clone());
            let hasher_quote = hasher_stream(estimated_bytes);
            key_quote = hash_fields_stream(fields, hasher_quote);
        } else {
            // If no keys attr was given, use all valid fields of the struct instead..
            let mut fields = Vec::new();
            for field in s.fields.iter() {
                match field.ident {
                    Some(ref ident) => {
                        let mut ignore = false;
                        // NOTE: ignore attempting to hash on Vec and Option values
                        //
                        // Generated Option's may not have Hash implemented
                        if let syn::Type::Path(p) = &field.ty {
                            for segment in &p.path.segments {
                                let ref ident = segment.ident;
                                let cleaned = ident.to_string().to_lowercase();
                                if cleaned == "option" || cleaned == "vec" {
                                    ignore = true;
                                }
                            }
                        }

                        if !ignore {
                            fields.push(ident.to_string());
                        }
                    }
                    None => panic!("Struct missing identiy"),
                }
            }

            let estimated_bytes = bytes_estimation(s, fields.clone());
            let hasher_quote = hasher_stream(estimated_bytes);
            key_quote = hash_fields_stream(fields, hasher_quote);
        }
    } else if let syn::Data::Enum(..) = input.data {
        if keys.is_some() {
            panic!("Hashing keys only work for structs");
        }
        key_quote = quote! { 0 }; // make get_key return 0
    } else {
        panic!("#[derive(Arcon)] only works for structs/enums");
    }

    let output: proc_macro2::TokenStream = {
        quote! {
            impl #impl_generics ::arcon::ArconType for #name #ty_generics #where_clause {
                #(#ids)*

                #[inline]
                fn get_key(&self) -> u64 {
                    #key_quote
                }
            }
        }
    };

    proc_macro::TokenStream::from(output)
}

/// Estimate the required bytes for hashing a struct given the selected `fields`
fn bytes_estimation(s: &syn::DataStruct, fields: Vec<String>) -> usize {
    let mut bytes = 0;
    for field in s.fields.iter() {
        match field.ident {
            Some(ref ident) => {
                if fields.contains(&ident.to_string()) {
                    if let syn::Type::Path(p) = &field.ty {
                        for segment in &p.path.segments {
                            let ref ident = segment.ident;
                            let cleaned = ident.to_string();
                            if cleaned == STRING_IDENT {
                                bytes += STRING_BYTES_ESTIMATION;
                            } else if cleaned == U32_IDENT {
                                bytes += std::mem::size_of::<u32>();
                            } else if cleaned == U64_IDENT {
                                bytes += std::mem::size_of::<u64>();
                            } else if cleaned == I32_IDENT {
                                bytes += std::mem::size_of::<i32>();
                            } else if cleaned == I64_IDENT {
                                bytes += std::mem::size_of::<i64>();
                            } else if cleaned == BOOL_IDENT {
                                bytes += std::mem::size_of::<bool>();
                            } else if cleaned == VEC_IDENT {
                                if let syn::PathArguments::AngleBracketed(ag) = &segment.arguments {
                                    for a in ag.args.iter() {
                                        if let syn::GenericArgument::Type(t) = a {
                                            if let syn::Type::Path(tp) = t {
                                                for s in &tp.path.segments {
                                                    if s.ident == U8_IDENT {
                                                        bytes += BYTE_ARRAY_ESTIMATION;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            None => panic!("Struct missing identiy"),
        }
    }

    bytes
}

/// Return a TokenStream with a Hasher implementation based on the estimated bytes
fn hasher_stream(estimated_bytes: usize) -> proc_macro2::TokenStream {
    // Selection Process:
    //
    // Low bytes estimation: Pick a hasher suited for small values
    // Medium bytes estimation: Pick Rust's default hasher
    // High bytes estimation: Pick XXHash that performs much better on larger values

    if estimated_bytes <= 8 {
        quote! { ::arcon::FxHasher::default(); }
    } else if estimated_bytes <= 32 {
        quote! { ::std::collections::hash_map::DefaultHasher::new(); }
    } else {
        quote! { ::arcon::XxHash64::default(); }
    }
}

/// Return the body of a function that hashes on a number of fields
///
/// The output of the function is a key in the form of a [u64].
fn hash_fields_stream(
    keys: Vec<String>,
    hasher: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let keys: Vec<proc_macro2::TokenStream> = keys
        .into_iter()
        .map(|k| {
            let struct_field = Ident::new(&k.trim(), Span::call_site());
            quote! { self.#struct_field.hash(&mut state); }
        })
        .collect();

    quote! {
        use ::std::hash::{Hash, Hasher};
        let mut state = #hasher
        #(#keys)*
        state.finish()
    }
}

/// Collect arcon attrs #[arcon(..)] meta list
fn arcon_attr_meta(
    meta_list: syn::MetaList,
) -> (Option<u64>, Option<u64>, Option<u32>, Option<String>) {
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
    (unsafe_ser_id, reliable_ser_id, version, keys)
}

/// Collect arcon attrs from doc comments
fn arcon_doc_attr(
    name_values: Vec<syn::Meta>,
) -> (Option<u64>, Option<u64>, Option<u32>, Option<String>) {
    let mut unsafe_ser_id = None;
    let mut reliable_ser_id = None;
    let mut version = None;
    let mut keys = None;

    for attr in name_values {
        let lit = match attr {
            syn::Meta::NameValue(v) => v.lit,
            _ => panic!("expected NameValue"),
        };

        if let syn::Lit::Str(s) = lit {
            let value = s.value();
            let attr_str = value.trim();
            let str_parts: Vec<String> = attr_str
                .clone()
                .trim()
                .to_string()
                .split(" ")
                .map(|s| s.to_string())
                .collect();

            if str_parts.len() == 3 {
                if str_parts[1] == "=" {
                    if str_parts[0] == "unsafe_ser_id" {
                        unsafe_ser_id = Some(str_parts[2].parse::<u64>().unwrap());
                    } else if str_parts[0] == "reliable_ser_id" {
                        reliable_ser_id = Some(str_parts[2].parse::<u64>().unwrap());
                    } else if str_parts[0] == "version" {
                        version = Some(str_parts[2].parse::<u32>().unwrap());
                    } else if str_parts[0] == "keys" {
                        keys = Some(str_parts[2].to_string())
                    }
                }
            }
        } else {
            panic!("unsafe_ser_id must be an Str literal");
        }
    }
    (unsafe_ser_id, reliable_ser_id, version, keys)
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
