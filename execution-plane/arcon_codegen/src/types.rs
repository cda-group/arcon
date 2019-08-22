use crate::GENERATED_STRUCTS;
use proc_macro2::{Ident, Span, TokenStream};
use spec::Type::*;
use spec::{MergeOp, Scalar, Type, Type::Struct};

pub fn to_token_stream(t: &Type, spec_id: &String) -> TokenStream {
    match t {
        Scalar(s) => {
            let ident = Ident::new(scalar(s), Span::call_site());
            quote! { #ident }
        }
        Struct {
            id,
            key,
            decoder,
            field_tys,
        } => {
            let generated = struct_gen(&id, *key, &decoder, *&field_tys, spec_id).to_string();
            let mut struct_map = GENERATED_STRUCTS.lock().unwrap();
            let struct_ident = Ident::new(id, Span::call_site());
            if let Some(map) = struct_map.get_mut(spec_id) {
                if !map.contains_key(id) {
                    map.insert(
                        String::from(id),
                        generated,
                    );
                }
            }
            quote! { #struct_ident }
        }
        Appender { elem_ty } => {
            let t = to_token_stream(&elem_ty, spec_id);
            quote! { Appender<#t> }
        }
        Merger { elem_ty: _, op: _ } => {
            unimplemented!();
        }
        Vector { elem_ty } => {
            let v = to_token_stream(&elem_ty, spec_id);
            quote! { ArconVec<#v> }
        }
        DictMerger { key_ty, val_ty, op } => {
            let op_ident = Ident::new(&merge_op(op), Span::call_site());
            let key = to_token_stream(&key_ty, spec_id);
            let val = to_token_stream(&val_ty, spec_id);
            quote! { DictMerger<#key, #val, #op_ident> }
        }
        GroupMerger { key_ty, val_ty } => {
            let key = to_token_stream(&key_ty, spec_id);
            let val = to_token_stream(&val_ty, spec_id);
            quote! { GroupMerger<#key, #val> }
        }
        VecMerger { elem_ty: _, op: _ } => {
            unimplemented!();
        }
    }
}

fn scalar(scalar: &Scalar) -> &str {
    match scalar {
        Scalar::I8 => "i8",
        Scalar::I16 => "i16",
        Scalar::I32 => "i32",
        Scalar::I64 => "i64",
        Scalar::U8 => "u8",
        Scalar::U16 => "u16",
        Scalar::U32 => "u32",
        Scalar::U64 => "u64",
        Scalar::F32 => "f32",
        Scalar::F64 => "f64",
        Scalar::Bool => "WeldBool",
        Scalar::Unit => "()",
    }
}

fn merge_op(op: &MergeOp) -> String {
    match op {
        MergeOp::Plus => "+",
        MergeOp::Mult => "*",
        MergeOp::Max => "max",
        MergeOp::Min => "min",
    }
    .to_string()
}

fn struct_gen(
    id: &str,
    key: Option<u32>,
    decoder: &Option<String>,
    field_tys: &Vec<Type>,
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

    #[test]
    fn struct_gen_test() {
        let s = Struct {
            id: String::from("MyStruct"),
            key: Some(0),
            decoder: None,
            field_tys: vec![Scalar(Scalar::U32), Scalar(Scalar::I32)],
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
    }
}
