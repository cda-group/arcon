use proc_macro as pm;
use proc_macro2 as pm2;
use quote::quote;

pub fn derive_proto(item: pm::TokenStream) -> pm::TokenStream {
    let item: syn::Item = syn::parse_macro_input!(item as syn::Item);
    match item {
        syn::Item::Enum(item) => rewrite_enum(item),
        syn::Item::Struct(item) => rewrite_struct(item),
        _ => panic!("#[proto] expects enum or struct as input"),
    }
}

/// Rewrites an enum into a `prost::Message` struct which wraps the enum (`prost::Oneof`).
fn rewrite_enum(mut item: syn::ItemEnum) -> pm::TokenStream {
    let enum_ident = syn::Ident::new(&format!("{}Enum", item.ident), pm2::Span::call_site());
    let struct_ident = std::mem::replace(&mut item.ident, enum_ident.clone());
    item.variants
        .iter_mut()
        .enumerate()
        .for_each(|(tag, variant)| {
            assert!(
                variant.fields.len() == 1,
                "#[proto] expects variant fields to take exactly one argument"
            );
            let field = variant.fields.iter_mut().next().unwrap();
            let (attr, is_unit) = get_prost_field_attr(&field.ty, Some(tag));
            variant.attrs.push(attr);
            if is_unit {
                field.ty = syn::parse_quote!(arcon::prelude::ArconUnit)
            }
        });
    let vis = item.vis.clone();
    let tags = item
        .variants
        .iter()
        .enumerate()
        .map(|(i, _)| format!("{}", i))
        .collect::<Vec<_>>()
        .join(",");
    let mut attrs = item.attrs.clone();
    let tags = syn::LitStr::new(&tags, pm2::Span::call_site());
    let enum_ident_str = syn::LitStr::new(&enum_ident.to_string(), enum_ident.span());
    let mut struct_item: syn::ItemStruct = syn::parse_quote! {
        #vis struct #struct_ident {
            #[prost(oneof = #enum_ident_str, tags = #tags)]
            #vis this: Option<#enum_ident>
        }
    };
    struct_item.attrs.append(&mut attrs);
    quote!(
        #[derive(prost::Message)]
        #struct_item
        #[derive(prost::Oneof)]
        #item
        use #enum_ident::*;
        impl #enum_ident {
            #vis fn wrap(self) -> #struct_ident {
                #struct_ident { this: Some(self) }
            }
        }
    )
    .into()
}

/// Rewrites a struct into a `prost::Message`.
fn rewrite_struct(mut item: syn::ItemStruct) -> pm::TokenStream {
    item.fields.iter_mut().for_each(|field| {
        let (attr, is_unit) = get_prost_field_attr(&field.ty, None);
        field.attrs.push(attr);
        if is_unit {
            field.ty = syn::parse_quote!(arcon::prelude::ArconUnit)
        }
    });
    quote!(
        #[derive(prost::Message)]
        #item
    )
    .into()
}

/// Synthesizes a prost field-attribute with an optional tag from a Rust type.
/// For example:
///
/// * `ty = i32`, `tag = None` becomes `#[prost(int32)]`.
/// * `ty = i32`, `tag = Some(1)` becomes `#[prost(int32, tag = 1)]`.
///
/// Also returns a `bool` which is `true` if the field-type is `()` and `false`
/// if it is not.
///
/// For prost-type conversions, see: https://github.com/danburkert/prost#fields.
fn get_prost_field_attr(ty: &syn::Type, tag: Option<usize>) -> (syn::Attribute, bool) {
    let mut is_unit = false;
    let ty = match &ty {
        syn::Type::Path(ty) => {
            let seg = ty.path.segments.iter().next().unwrap();
            match seg.ident.to_string().as_str() {
                "i32" => "int32",
                "i64" => "int64",
                "bool" => "bool",
                "f32" => "float",
                "f64" => "double",
                "u32" => "uint32",
                "u64" => "uint64",
                "String" => "string",
                // This case covers messages which are wrapped in Box<T> as well
                _ => "message",
            }
            .to_string()
        }
        syn::Type::Tuple(ty) if ty.elems.is_empty() => {
            is_unit = true;
            "message".to_string()
        }
        _ => panic!("#[proto] expects all types to be mangled and de-aliased."),
    };
    let ident = syn::Ident::new(&ty, pm2::Span::call_site());
    let attr = if let Some(tag) = tag {
        let lit = syn::LitStr::new(&format!("{}", tag), pm2::Span::call_site());
        syn::parse_quote!(#[prost(#ident, tag = #lit)])
    } else {
        syn::parse_quote!(#[prost(#ident, required)])
    };
    (attr, is_unit)
}
