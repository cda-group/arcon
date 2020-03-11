//! The arcon_macros crate contains macros used by [arcon].

#![recursion_limit = "128"]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::{parse_macro_input, DeriveInput};

/// arcon is a macro that helps define a Arcon supported struct
///
/// By default, the macro will implement [std::hash::Hasher] that hashes on all fields of the
/// struct. Use [arcon_keyed] instead if the struct needs to be hashed on specific fields.
#[proc_macro_attribute]
pub fn arcon(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;
    let _ = proc_macro2::TokenStream::from(metadata);

    if let syn::Data::Struct(ref s) = item.data {
        let generics = &item.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

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
                impl #impl_generics ArconType for #name #ty_generics #where_clause {}
                impl ::std::hash::Hash for #name {
                    fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
                        // will make it hash on all fields of the struct...
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

/// arcon_keyed constructs an Arcon defined struct with custom hasher
///
/// `#[arcon_keyed(id)]` will attempt to hash the struct using only the `id` field
#[proc_macro_attribute]
pub fn arcon_keyed(keys: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;

    if let syn::Data::Struct(_) = item.data {
        let generics = &item.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let keys: Vec<proc_macro2::TokenStream> = keys
            .to_string()
            .trim()
            .split(',')
            .map(|k| {
                let struct_field = Ident::new(&k.trim(), Span::call_site());
                quote! { self.#struct_field.hash(state); }
            })
            .collect();

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
                impl #impl_generics ArconType for #name #ty_generics #where_clause {}
                impl ::std::hash::Hash for #name {
                    fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
                        #(#keys)*
                    }
                }
            }
        };

        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[arcon_keyed] is only defined for structs!");
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
