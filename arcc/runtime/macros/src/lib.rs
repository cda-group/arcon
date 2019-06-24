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
            use std::hash::Hasher;
            use std::hash::Hash;
            #item
            impl Hash for #name {
                fn hash<H: Hasher>(&self, state: &mut H) {
                    self.#key.hash(state);
                }
            }
        }
    };

    proc_macro::TokenStream::from(output)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    #[repr(C)]
    #[key_by(id)]
    pub struct Item {
        id: u64,
        price: u32,
    }

    #[test]
    fn key_by_test() {
        let i1 = Item {
            id: 1,
            price: 20,
        };
        let i2 = Item {
            id: 2,
            price: 150,
        };
        let i1 = Item {
            id: 1,
            price: 50,
        };

    }

    fn calc_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }
}
