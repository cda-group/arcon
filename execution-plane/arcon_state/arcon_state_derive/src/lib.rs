#![recursion_limit = "128"]
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(ArconState, attributes(ephemeral))]
pub fn arcon_state(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;

    if let syn::Data::Struct(ref s) = item.data {
        let mut idents = Vec::new();
        let mut ephemerals = Vec::new();

        match s.fields {
            syn::Fields::Named(ref fields_named) => {
                for field in fields_named.named.iter() {
                    let mut ephemeral= false;
                    for attr in field.attrs.iter() {
                        let meta = attr.parse_meta().unwrap();
                        match meta {
                            syn::Meta::Path(ref path)
                                if path.get_ident().unwrap().to_string() == "ephemeral" =>
                            {
                                idents.push((field.ident.clone(), &field.ty));
                                ephemerals.push((field.ident.clone(), &field.ty));
                                ephemeral = true;
                            }
                            _ => (),
                        }
                    }
                    if !ephemeral {
                        idents.push((field.ident.clone(), &field.ty));
                    }
                }
            }
            _ => {}
        }

        let mut field_getters = Vec::new();
        let mut persist_quotes = Vec::new();

        for data in idents.into_iter() {
            let ident = &data.0;
            let ty = &data.1;

            // add only non-ephemeral fields
            if !ephemerals.contains(&(data)) {
                let field_gen = quote! { self.#ident.persist()?; };
                persist_quotes.push(field_gen);
            }

            let field_gen = quote! { pub fn #ident(&mut self) -> &mut #ty { &mut self.#ident } };
            field_getters.push(field_gen);
        }

        let generics = &item.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let output: proc_macro2::TokenStream = {
            quote! {
                impl #impl_generics ::arcon_state::index::ArconState for #name #ty_generics #where_clause {}

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
