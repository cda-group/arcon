use proc_macro2::{Ident, Span, TokenStream};
use spec::Type::*;
use spec::{MergeOp, Scalar, Type};

pub fn to_token_stream(t: &Type) -> TokenStream {
    match t {
        Scalar(s) => {
            let ident = Ident::new(scalar(s), Span::call_site());
            quote! { #ident }
        }
        Struct { field_tys: _ } => {
            unimplemented!();
        }
        Appender { elem_ty } => {
            let t = to_token_stream(&elem_ty);
            quote! { Appender<#t> }
        }
        Merger { elem_ty: _, op: _ } => {
            unimplemented!();
        }
        Vector { elem_ty } => {
            let v = to_token_stream(&elem_ty);
            quote! { ArconVec<#v> }
        }
        DictMerger { key_ty, val_ty, op } => {
            let op_ident = Ident::new(&merge_op(op), Span::call_site());
            let key = to_token_stream(&key_ty);
            let val = to_token_stream(&val_ty);
            quote! { DictMerger<#key, #val, #op_ident> }
        }
        GroupMerger { key_ty, val_ty } => {
            let key = to_token_stream(&key_ty);
            let val = to_token_stream(&val_ty);
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
