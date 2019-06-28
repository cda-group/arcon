extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields};

#[proc_macro_attribute]
pub fn key_by(key: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;

    let _fields = match &item.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => &fields.named,
        _ => panic!("expected a struct with named fields"),
    };

    let key = proc_macro2::TokenStream::from(key);

    let output: proc_macro2::TokenStream = {
        quote! {
            #item
            impl ::std::hash::Hash for #name {
                fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
                    self.#key.hash(state);
                }
            }
        }
    };

    proc_macro::TokenStream::from(output)
}
