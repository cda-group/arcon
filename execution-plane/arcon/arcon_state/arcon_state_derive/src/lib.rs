#![recursion_limit = "128"]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(ArconState)]
pub fn arcon_state(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;

    if let syn::Data::Struct(ref s) = item.data {
        let mut idents = Vec::new();
        for field in s.fields.iter() {
            match field.ident {
                Some(ref ident) => idents.push((ident.clone(), &field.ty)),
                None => panic!("Struct missing identiy"),
            }
        }

        let mut persist_quotes = Vec::new();
        for (ident, _) in idents.iter() {
            let field_gen = quote! { self.#ident.persist()?; };
            persist_quotes.push(field_gen);
        }

        let mut field_getters = Vec::new();
        for (ident, ty) in idents.iter() {
            let field_gen = quote! { pub fn #ident(&mut self) -> &mut #ty { &mut self.#ident } };
            field_getters.push(field_gen);
        }

        let generics = &item.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let output: proc_macro2::TokenStream = {
            quote! {
                impl #impl_generics ::arcon_state::index::ActiveState for #name #ty_generics #where_clause {}

                impl #impl_generics ::arcon_state::index::IndexOps for #name #ty_generics #where_clause {
                    #[inline]
                    fn persist(&mut self) -> Result<(), ::arcon_state::error::ArconStateError> {
                        #(#persist_quotes)*
                        Ok(())
                    }
                }

                impl #impl_generics #name #ty_generics #where_clause {
                    #(#field_getters)*
                }
            }
        };

        return proc_macro::TokenStream::from(output);
    } else {
        panic!("#[derive(ArconState)] only works for structs");
    }
}
