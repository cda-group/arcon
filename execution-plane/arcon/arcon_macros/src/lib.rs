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
                #[derive(Clone, Abomonation)]
                #item
                impl #impl_generics ArconType for #name #ty_generics #where_clause {}
            }
        };

        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[arcon] is only defined for structs!");
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
