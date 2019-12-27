use crate::GENERATED_STRUCTS;
use arcon_proto::arcon_spec::type_holder::ArconType;
use arcon_proto::arcon_spec::scalar;
use arcon_proto::arcon_spec::{TypeHolder, Scalar};
use proc_macro2::{Ident, Span, TokenStream};

pub fn to_token_stream(t: &TypeHolder, spec_id: &String) -> TokenStream {
    match &t.arcon_type {
        Some(ArconType::Scalar(s)) => {
            let ident = Ident::new(scalar(s), Span::call_site());
            quote! { #ident }
        }
        Some(ArconType::Struct(s)) => {
            let generated =
                struct_gen(&s.id, Some(s.key), &None, &s.field_tys, spec_id).to_string();
            let mut struct_map = GENERATED_STRUCTS.lock().unwrap();
            let struct_ident = Ident::new(&s.id, Span::call_site());
            if let Some(map) = struct_map.get_mut(spec_id) {
                if !map.contains_key(&s.id) {
                    map.insert(String::from(s.id.clone()), generated);
                }
            }
            quote! { #struct_ident }
        }
        Some(ArconType::Vec(v)) => {
            let ident = to_token_stream(&v.arcon_type.clone().unwrap(), spec_id);
            quote! { Vec<#ident> }
        }
        Some(ArconType::Str(_)) => {
            quote! { String }
        }
        None => {
            panic!("Failed to match ArconType");
        }
    }
}

/// Emm, yeah..
fn scalar(s: &str) -> &str {
    match s {
        "i32" => "i32",
        "i64" => "i64",
        "u32" => "u32",
        "u64" => "u64",
        "f32" => "f32",
        "f64" => "f64",
        "bool" => "bool",
        _ => panic!("Bad Scalar value"),
    }
}

fn struct_gen(
    id: &str,
    key: Option<u32>,
    decoder: &Option<String>,
    field_tys: &Vec<TypeHolder>,
    spec_id: &String,
) -> TokenStream {
    let key_opt_stream = if let Some(k) = key {
        let key_id = format!("f{}", k);
        let key_ident = Ident::new(&key_id, Span::call_site());
        Some(quote! { #[key_by(#key_ident)] })
    } else {
        None
    };

    // Default decoding by comma
    let decoder_stream = if let Some(dec) = decoder {
        let dec_ident = Ident::new(&dec.trim(), Span::call_site());
        Some(quote! { #[arcon_decoder(#dec_ident)] })
    } else {
        Some(quote! { #[arcon_decoder(,)] })
    };

    let mut field_counter: u32 = 0;
    let mut fields: Vec<TokenStream> = Vec::new();

    let struct_ident = Ident::new(id, Span::call_site());

    for field in &field_tys.clone() {
        let expanded_field = to_token_stream(field, spec_id);
        let field_str = format!("f{}", field_counter);
        let field_ident = Ident::new(&field_str, Span::call_site());
        let field_quote = quote! { #field_ident: #expanded_field };
        fields.push(field_quote);
        field_counter += 1;
    }

    let struct_def = quote! { #struct_ident {
        #(#fields),*
    } };

    // TODO: Just temporary solution! Remove!
    if key.is_some() {
        quote! {
            #key_opt_stream
            #decoder_stream
            #[arcon]
            pub struct #struct_def
        }
    } else {
        quote! {
            #decoder_stream
            #[arcon]
            pub struct #struct_def
            impl ::std::hash::Hash for #struct_ident {
                fn hash<H: ::std::hash::Hasher>(&self, _state: &mut H) {

                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arcon_proto::arcon_spec::Struct;

    /*
    #[test]
    fn struct_gen_test() {
        let s = Struct {
            id: String::from("MyStruct"),
            key: Some(0),
            decoder: None,
            field_tys: vec![Types::Scalar::U32, Types::Scalar::I32],
        };
        // Should generate the following struct
        //
        // #[key_by(f0)]
        // #[arcon_decoder(,)]
        // #[arcon]
        // pub struct MyStruct {
        //  f0: u32,
        //  f1: i32,
        // }
        /*
        match s {
            Struct {
                id,
                key,
                decoder,
                field_tys,
            } => {
                let stream = struct_gen(&id, key, &decoder, &field_tys, &"MySpec".to_string());
                let fmt = crate::format_code(stream.to_string()).unwrap();
                // RustFmt will return an empty String if it is bad Rust code...
                assert!(fmt.len() > 0);
                let expected = String::from("#[key_by(f0)]\n# [ arcon_decoder ( , ) ]\n#[arcon]\npub struct MyStruct {\n    f0: u32,\n    f1: i32,\n}\n");
                assert_eq!(expected, fmt);
            }
            _ => panic!("Not supposed to happen"),
        }
        */
    }
*/
}
