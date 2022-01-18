use proc_macro::TokenStream;
use quote::ToTokens;
use syn::DeriveInput;

pub fn derive_arcon(input: TokenStream) -> TokenStream {
    let input: DeriveInput = syn::parse(input).unwrap();
    let name = &input.ident;

    #[allow(unused)]
    let (unsafe_ser_id, reliable_ser_id, version) = {
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

    let reliable_ser_id = reliable_ser_id.expect("missing reliable_ser_id attr");
    let version = version.expect("missing version attr");
    #[cfg(feature = "unsafe_flight")]
    let unsafe_ser_id = unsafe_ser_id.expect("missing unsafe_ser_id attr");

    #[cfg(feature = "unsafe_flight")]
    // Id check
    assert_ne!(
        unsafe_ser_id, reliable_ser_id,
        "UNSAFE_SER_ID and RELIABLE_SER_ID must have different values"
    );

    let mut ids: Vec<proc_macro2::TokenStream> = Vec::with_capacity(3);
    ids.push(quote! { const RELIABLE_SER_ID: ::arcon::SerId  = #reliable_ser_id; });
    ids.push(quote! { const VERSION_ID: ::arcon::VersionId = #version; });
    #[cfg(feature = "unsafe_flight")]
    ids.push(quote! { const UNSAFE_SER_ID: ::arcon::SerId = #unsafe_ser_id; });

    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let output: proc_macro2::TokenStream = {
        quote! {
            impl #impl_generics ::arcon::ArconType for #name #ty_generics #where_clause {
                #(#ids)*
            }
        }
    };

    proc_macro::TokenStream::from(output)
}

/// Collect arcon attrs #[arcon(..)] meta list
fn arcon_attr_meta(meta_list: syn::MetaList) -> (Option<u64>, Option<u64>, Option<u32>) {
    let mut unsafe_ser_id = None;
    let mut reliable_ser_id = None;
    let mut version = None;

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
        } else {
            panic!(
                "unsupported attribute key '{}' found",
                pair.path.to_token_stream()
            )
        }
    }
    (unsafe_ser_id, reliable_ser_id, version)
}

/// Collect arcon attrs from doc comments
fn arcon_doc_attr(name_values: Vec<syn::Meta>) -> (Option<u64>, Option<u64>, Option<u32>) {
    let mut unsafe_ser_id = None;
    let mut reliable_ser_id = None;
    let mut version = None;

    for attr in name_values {
        let lit = match attr {
            syn::Meta::NameValue(v) => v.lit,
            _ => panic!("expected NameValue"),
        };

        if let syn::Lit::Str(s) = lit {
            let value = s.value();
            let attr_str = value.trim();
            let str_parts: Vec<String> = attr_str
                .trim()
                .to_string()
                .split(' ')
                .map(|s| s.to_string())
                .collect();

            if str_parts.len() == 3 && str_parts[1] == "=" {
                if str_parts[0] == "unsafe_ser_id" {
                    unsafe_ser_id = Some(str_parts[2].parse::<u64>().unwrap());
                }

                if str_parts[0] == "reliable_ser_id" {
                    reliable_ser_id = Some(str_parts[2].parse::<u64>().unwrap());
                } else if str_parts[0] == "version" {
                    version = Some(str_parts[2].parse::<u32>().unwrap());
                }
            }
        } else {
            panic!("unsafe_ser_id must be an Str literal");
        }
    }
    (unsafe_ser_id, reliable_ser_id, version)
}
