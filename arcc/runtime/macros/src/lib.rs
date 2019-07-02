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
        let output: proc_macro2::TokenStream = {
            quote! {
                #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
                #[repr(C)]
                #item
                impl crate::data::ArconType for #name {}
            }
        };
        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[arcon] is only defined for structs!");
    }
}
