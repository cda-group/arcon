use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

pub fn derive_state(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as DeriveInput);
    let name = &item.ident;

    if let syn::Data::Struct(ref s) = item.data {
        let mut tables = Vec::new();
        let mut table_ids = Vec::new();
        let mut table_lookups = Vec::new();

        let mut field_getters = Vec::new();
        let mut persist_quotes = Vec::new();
        let mut key_quotes = Vec::new();

        if let syn::Fields::Named(ref fields_named) = s.fields {
            for field in fields_named.named.iter() {
                let mut ephemeral = false;
                let ident = &field.ident;
                let ty = &field.ty;
                for attr in field.attrs.iter() {
                    ephemeral = is_ephemeral(attr);
                    {
                        match get_table(attr, ident) {
                            Ok(Some(quote)) => {
                                assert!(
                                    !ephemeral,
                                    "Cannot use ephemeral attribute with table attribute"
                                );
                                let table_id = get_table_id(attr).unwrap();
                                table_lookups.push(quote! { #table_id => self.#ident.table(), });
                                table_ids.push(quote! { #table_id.to_string() });
                                tables.push(quote)
                            }
                            Ok(None) => (),
                            Err(err) => panic!("{}", err),
                        }
                    }
                }

                if !ephemeral {
                    let field_gen = quote! { self.#ident.persist()?; };
                    persist_quotes.push(field_gen);
                    let field_gen = quote! { self.#ident.set_key(key); };
                    key_quotes.push(field_gen);
                }

                field_getters
                    .push(quote! { pub fn #ident(&mut self) -> &mut #ty { &mut self.#ident } });
            }
        }
        let generics = &item.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let has_tables_quote = {
            if tables.is_empty() {
                quote! { false }
            } else {
                quote! { true }
            }
        };

        let tables = quote! {
            #[inline]
            fn tables(&mut self) -> Vec<::arcon::ImmutableTable> {
                vec![#(#tables)*]
                    .into_iter()
                    .filter_map(|m| m)
                    .collect::<Vec<::arcon::ImmutableTable>>()
            }
            fn has_tables() -> bool {
                #has_tables_quote
            }
            fn get_table(&mut self, id: &str) -> ::arcon::ArconResult<Option<::arcon::ImmutableTable>> {
                match id {
                    #(#table_lookups)*
                    _ => Ok(None),
                }
            }
            fn table_ids() -> Vec<String> {
                vec![#(#table_ids)*]
            }
        };

        let output: proc_macro2::TokenStream = {
            quote! {
                impl #impl_generics ::arcon::ArconState for #name #ty_generics #where_clause {
                    const STATE_ID: &'static str = stringify!(#name);

                    #[inline]
                    fn persist(&mut self) -> ::arcon::ArconResult<()> {
                        #(#persist_quotes)*
                        Ok(())
                    }
                    #[inline]
                    fn set_key(&mut self, key: u64) {
                        #(#key_quotes)*
                    }

                    #tables
                }

                impl #impl_generics #name #ty_generics #where_clause {
                    #(#field_getters)*
                }
            }
        };

        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[derive(ArconState)] only works for structs");
    }
}

fn is_ephemeral(attr: &syn::Attribute) -> bool {
    attr.path.is_ident("ephemeral")
}

fn get_table_id(attr: &syn::Attribute) -> syn::Result<syn::LitStr> {
    match attr.parse_meta()? {
        syn::Meta::NameValue(syn::MetaNameValue {
            lit: syn::Lit::Str(table_name),
            ..
        }) => Ok(table_name),
        _ => {
            let message = "expected #[table = \"...\"]";
            Err(syn::Error::new_spanned(attr, message))
        }
    }
}

fn get_table(
    attr: &syn::Attribute,
    ident: &Option<syn::Ident>,
) -> syn::Result<Option<proc_macro2::TokenStream>> {
    if !attr.path.is_ident("table") {
        return Ok(None);
    }

    match attr.parse_meta()? {
        syn::Meta::NameValue(syn::MetaNameValue {
            lit: syn::Lit::Str(table_name),
            ..
        }) => {
            let quote = quote! {
                match self.#ident.table() {
                    Ok(table) => table.and_then(|mut t| { t.set_name(#table_name); Some(t)}),
                    Err(_) => None,
                }
            };

            Ok(Some(quote))
        }
        _ => {
            let message = "expected #[table = \"...\"]";
            Err(syn::Error::new_spanned(attr, message))
        }
    }
}
