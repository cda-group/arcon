use proc_macro::TokenStream;
use syn::DeriveInput;

pub fn derive_arrow(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let name = &input.ident;

    if let syn::Data::Struct(ref s) = input.data {
        let mut arrow_types = Vec::new();
        let mut builders = Vec::new();

        if let syn::Fields::Named(ref fields_named) = s.fields {
            for (field_pos, field) in fields_named.named.iter().enumerate() {
                let ident = field.ident.clone();
                let ty = &field.ty;
                let arrow_quote = quote! { ::arcon::Field::new(stringify!(#ident), <#ty as ToArrow>::arrow_type(), false), };
                arrow_types.push(arrow_quote);

                let builder_quote = quote! {
                    {
                        let value = self.#ident;
                        match builder.field_builder::<<#ty as ToArrow>::Builder>(#field_pos) {
                            Some(b) => b.append_value(value)?,
                            None => return Err(::arcon::ArrowError::SchemaError(format!("Failed to downcast Arrow Builder"))),
                        }
                    }
                };
                builders.push(builder_quote);
            }
        } else {
            panic!("#[derive(Arrow)] requires named fields");
        }

        let generics = &input.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let fields: proc_macro2::TokenStream = {
            quote! {
                vec![#(#arrow_types)*]
            }
        };

        let output: proc_macro2::TokenStream = {
            quote! {
                impl #impl_generics ::arcon::ToArrow for #name #ty_generics #where_clause {
                    type Builder = ::arcon::StructBuilder;

                    fn arrow_type() -> ::arcon::DataType {
                        ::arcon::DataType::Struct(#fields)
                    }
                    fn schema() -> ::arcon::Schema {
                        ::arcon::Schema::new(#fields)
                    }
                    fn append(self, builder: &mut ::arcon::StructBuilder) -> Result<(), ::arcon::ArrowError> {
                        #(#builders)*
                        Ok(())
                    }
                    fn table() -> ::arcon::MutableTable {
                        let builder = ::arcon::StructBuilder::from_fields(#fields, ::arcon::RECORD_BATCH_SIZE);
                        let table_name = stringify!(#name).to_lowercase();
                        ::arcon::MutableTable::new(::arcon::RecordBatchBuilder::new(table_name, Self::schema(), builder))
                    }
                }
            }
        };

        proc_macro::TokenStream::from(output)
    } else {
        panic!("#[derive(Arrow)] only works for structs");
    }
}
