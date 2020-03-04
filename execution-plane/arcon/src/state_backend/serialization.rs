// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#![allow(non_snake_case)]

use crate::prelude::ArconResult;
use bytes::{Buf, BufMut};
use prost::Message;
use std::{
    any::type_name,
    collections::hash_map::DefaultHasher,
    hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
};

pub trait SerializableWith<S>: Sized {
    fn serialize(serializer: &S, payload: &Self) -> ArconResult<Vec<u8>>;
    fn serialize_into(serializer: &S, target: impl BufMut, payload: &Self) -> ArconResult<()>;

    fn size_hint(serializer: &S, payload: &Self) -> Option<usize>;
}

pub trait DeserializableWith<S>: Sized {
    fn deserialize(serializer: &S, bytes: &[u8]) -> ArconResult<Self>;
    fn deserialize_from(serializer: &S, source: impl Buf) -> ArconResult<Self>;
}

/// unsafe: implementations must assure that Self always serializes to buffers of size Self::SIZE
pub unsafe trait SerializableFixedSizeWith<S>: SerializableWith<S> {
    const SIZE: usize;
    fn serialize_check(serializer: &S, payload: &Self) -> ArconResult<Vec<u8>> {
        let serialized = Self::serialize(serializer, payload)?;
        assert_eq!(serialized.len(), Self::SIZE);
        Ok(serialized)
    }
}

#[cfg(feature = "arcon_serde")]
mod bincode {
    use super::*;
    use bytes::buf::ext::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Default, Copy, Clone)]
    pub struct Bincode;

    impl<T> SerializableWith<Bincode> for T
    where
        T: Serialize,
    {
        fn serialize(_serializer: &Bincode, payload: &Self) -> ArconResult<Vec<u8>> {
            bincode::serialize(payload).map_err(|e| {
                arcon_err_kind!(
                    "Could not serialize payload of type `{}`: {}",
                    type_name::<Self>(),
                    e
                )
            })
        }

        fn serialize_into(
            _serializer: &Bincode,
            target: impl BufMut,
            payload: &Self,
        ) -> ArconResult<()> {
            bincode::serialize_into(target.writer(), payload).map_err(|e| {
                arcon_err_kind!(
                    "Could not serialize payload of type `{}`: {}",
                    type_name::<Self>(),
                    e
                )
            })
        }

        fn size_hint(_serializer: &Bincode, payload: &Self) -> Option<usize> {
            bincode::serialized_size(payload).map(|x| x as usize).ok()
        }
    }

    impl<T> DeserializableWith<Bincode> for T
    where
        T: for<'de> Deserialize<'de>,
    {
        fn deserialize(_serializer: &Bincode, bytes: &[u8]) -> ArconResult<Self> {
            bincode::deserialize(bytes).map_err(|e| {
                arcon_err_kind!(
                    "Could not deserialize payload of type `{}`: {}",
                    type_name::<Self>(),
                    e
                )
            })
        }

        fn deserialize_from(_serializer: &Bincode, source: impl Buf) -> ArconResult<Self> {
            bincode::deserialize_from(source.reader()).map_err(|e| {
                arcon_err_kind!(
                    "Could not deserialize payload of type `{}`: {}",
                    type_name::<Self>(),
                    e
                )
            })
        }
    }

    macro_rules! impl_serializable_fixed_size {
        ($serialization_strategy: ty; $($t: ty : $size: expr),+) => {
            $(unsafe impl SerializableFixedSizeWith<$serialization_strategy> for $t {
                const SIZE: usize = $size;
            })+
        };
    }

    impl_serializable_fixed_size!(Bincode;
        u8: 1, u16: 2, u32: 4, u64: 8, usize: std::mem::size_of::<usize>(), u128: 16,
        i8: 1, i16: 2, i32: 4, i64: 8, isize: std::mem::size_of::<isize>(), i128: 16,
        f32: 4, f64: 8,
        char: 1, bool: 1, (): 0
    );
}
#[cfg(target = "arcon_serde")]
pub use self::bincode::Bincode;
use prost::encoding::encoded_len_varint;

#[derive(Debug, Default, Copy, Clone)]
pub struct Prost;
impl<T> SerializableWith<Prost> for T
where
    T: Message,
{
    fn serialize(serializer: &Prost, payload: &Self) -> ArconResult<Vec<u8>> {
        let size = Self::size_hint(serializer, payload).unwrap_or(0);
        let mut buf = Vec::with_capacity(size);
        payload.encode_length_delimited(&mut buf).map_err(|e| {
            arcon_err_kind!(
                "Could not serialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })?;
        Ok(buf)
    }

    fn serialize_into(
        _serializer: &Prost,
        mut target: impl BufMut,
        payload: &Self,
    ) -> ArconResult<()> {
        payload.encode_length_delimited(&mut target).map_err(|e| {
            arcon_err_kind!(
                "Could not serialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }

    fn size_hint(_serializer: &Prost, payload: &Self) -> Option<usize> {
        let len = payload.encoded_len();
        Some(len + encoded_len_varint(len as u64))
    }
}

impl<T> DeserializableWith<Prost> for T
where
    T: Message + Default,
{
    fn deserialize(_serializer: &Prost, bytes: &[u8]) -> ArconResult<Self> {
        <T as Message>::decode_length_delimited(bytes).map_err(|e| {
            arcon_err_kind!(
                "Could not deserialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }

    fn deserialize_from(_serializer: &Prost, source: impl Buf) -> ArconResult<Self> {
        <T as Message>::decode_length_delimited(source).map_err(|e| {
            arcon_err_kind!(
                "Could not deserialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }
}
// even integers have variable length encodings with protobuf, so we cannot implement SerializableFixedSizeWith for anything

// similar to bincode, but only for numeric primitives, strings, and vecs; with specified endianness
#[derive(Debug, Default, Copy, Clone)]
pub struct NativeEndianBytesDump;
#[derive(Debug, Default, Copy, Clone)]
pub struct BigEndianBytesDump;
#[derive(Debug, Default, Copy, Clone)]
pub struct LittleEndianBytesDump;

macro_rules! impl_byte_dump {
    ($serializer: ty, $to_bytes: ident, $from_bytes: ident; $($t: ty),+) => {$(
        impl SerializableWith<$serializer> for $t {
            fn serialize(_serializer: &$serializer, payload: &Self) -> ArconResult<Vec<u8>> {
                let bytes = payload.$to_bytes();
                let mut buf = vec![0; bytes.len()];
                buf.copy_from_slice(&bytes);
                Ok(buf)
            }

            fn serialize_into(
                _serializer: &$serializer,
                mut target: impl BufMut,
                payload: &Self,
            ) -> ArconResult<()> {
                let bytes = payload.$to_bytes();
                target.put_slice(&bytes);
                Ok(())
            }

            fn size_hint(_serializer: &$serializer, _payload: &Self) -> Option<usize> {
                Some(std::mem::size_of::<Self>())
            }
        }

        impl DeserializableWith<$serializer> for $t {
            fn deserialize(_serializer: &$serializer, bytes: &[u8]) -> ArconResult<Self> {
                let mut buf = [0; std::mem::size_of::<Self>()];
                buf.copy_from_slice(bytes);
                Ok(Self::$from_bytes(buf))
            }

            fn deserialize_from(_serializer: &$serializer, mut source: impl Buf) -> ArconResult<Self> {
                let mut buf = [0; std::mem::size_of::<Self>()];
                source.copy_to_slice(&mut buf);
                Ok(Self::$from_bytes(buf))
            }
        }

        unsafe impl SerializableFixedSizeWith<$serializer> for $t {
            const SIZE: usize = std::mem::size_of::<Self>();
        }
    )+};
}
macro_rules! impl_byte_dump_for_unit {
    ($serializer: ident) => {
        impl SerializableWith<$serializer> for () {
            fn serialize(_serializer: &$serializer, _payload: &Self) -> ArconResult<Vec<u8>> {
                Ok(vec![])
            }

            fn serialize_into(
                _serializer: &$serializer,
                _target: impl BufMut,
                _payload: &Self,
            ) -> ArconResult<()> {
                Ok(())
            }

            fn size_hint(_serializer: &$serializer, _payload: &Self) -> Option<usize> {
                Some(0)
            }
        }

        unsafe impl SerializableFixedSizeWith<$serializer> for () {
            const SIZE: usize = 0;
        }

        impl DeserializableWith<$serializer> for () {
            fn deserialize(_serializer: &$serializer, _bytes: &[u8]) -> ArconResult<Self> {
                Ok(())
            }

            fn deserialize_from(_serializer: &$serializer, _source: impl Buf) -> ArconResult<Self> {
                Ok(())
            }
        }
    };
}
macro_rules! impl_byte_dump_for_tuples {
    ($serializer: ident; $(($($T: ident),*)),*) => {$(
        impl<$($T: SerializableWith<$serializer>),*> SerializableWith<$serializer> for ($($T),*) {
            fn serialize(serializer: &$serializer, payload: &Self) -> ArconResult<Vec<u8>> {
                let size = 0 $(+ std::mem::size_of::<$T>())*;
                let mut res = Vec::with_capacity(size);
                let ($($T),*) = payload;
                $(
                    $T::serialize_into(serializer, &mut res, $T)?;
                )*
                Ok(res)
            }

            fn serialize_into(
                serializer: &$serializer,
                mut target: impl BufMut,
                payload: &Self,
            ) -> ArconResult<()> {
                let ($($T),*) = payload;
                $(
                    $T::serialize_into(serializer, &mut target, $T)?;
                )*
                Ok(())
            }

            fn size_hint(serializer: &$serializer, payload: &Self) -> Option<usize> {
                let ($($T),*) = payload;
                Some(0 $(+ $T::size_hint(serializer, $T).unwrap_or(0))*)
            }
        }

        unsafe impl<$($T: SerializableFixedSizeWith<$serializer>),*> SerializableFixedSizeWith<$serializer> for ($($T),*) {
            const SIZE: usize = 0 $(+ $T::SIZE)*;
        }

        impl<$($T: DeserializableWith<$serializer>),*> DeserializableWith<$serializer> for ($($T),*) {
            fn deserialize(serializer: &$serializer, bytes: &[u8]) -> ArconResult<Self> {
                Self::deserialize_from(serializer, bytes)
            }

            fn deserialize_from(serializer: &$serializer, mut source: impl Buf) -> ArconResult<Self> {
                Ok(($(
                    $T::deserialize_from(serializer, &mut source)?
                ),*))
            }
        }
    )*};
}
macro_rules! impl_byte_dump_for_node_id {
    ($serializer: ident) => {
        impl SerializableWith<$serializer> for crate::data::NodeID {
            fn serialize(serializer: &$serializer, payload: &Self) -> ArconResult<Vec<u8>> {
                u32::serialize(serializer, &payload.id)
            }

            fn serialize_into(
                serializer: &$serializer,
                target: impl BufMut,
                payload: &Self,
            ) -> ArconResult<()> {
                u32::serialize_into(serializer, target, &payload.id)
            }

            fn size_hint(serializer: &$serializer, payload: &Self) -> Option<usize> {
                u32::size_hint(serializer, &payload.id)
            }
        }

        unsafe impl SerializableFixedSizeWith<$serializer> for crate::data::NodeID {
            const SIZE: usize = <u32 as SerializableFixedSizeWith<$serializer>>::SIZE;
        }

        impl DeserializableWith<$serializer> for crate::data::NodeID {
            fn deserialize(serializer: &$serializer, bytes: &[u8]) -> ArconResult<Self> {
                let id = u32::deserialize(serializer, bytes)?;
                Ok(id.into())
            }

            fn deserialize_from(serializer: &$serializer, source: impl Buf) -> ArconResult<Self> {
                let id = u32::deserialize_from(serializer, source)?;
                Ok(id.into())
            }
        }
    };
}
macro_rules! impl_byte_dump_for_string {
    ($serializer: ident) => {
        impl SerializableWith<$serializer> for &'_ str {
            fn serialize(serializer: &$serializer, payload: &Self) -> ArconResult<Vec<u8>> {
                let len = payload.len();
                let mut res = Vec::with_capacity(std::mem::size_of_val(&len) + len);
                usize::serialize_into(serializer, &mut res, &len)?;
                res.extend_from_slice(payload.as_bytes());
                Ok(res)
            }

            fn serialize_into(
                serializer: &$serializer,
                mut target: impl BufMut,
                payload: &Self,
            ) -> ArconResult<()> {
                let len = payload.len();
                usize::serialize_into(serializer, &mut target, &len)?;
                target.put_slice(payload.as_bytes());
                Ok(())
            }

            fn size_hint(_serializer: &$serializer, payload: &Self) -> Option<usize> {
                Some(std::mem::size_of::<usize>() + payload.len())
            }
        }

        impl SerializableWith<$serializer> for String {
            fn serialize(serializer: &$serializer, payload: &Self) -> ArconResult<Vec<u8>> {
                <&str>::serialize(serializer, &payload.as_str())
            }

            fn serialize_into(
                serializer: &$serializer,
                target: impl BufMut,
                payload: &Self,
            ) -> ArconResult<()> {
                <&str>::serialize_into(serializer, target, &payload.as_str())
            }

            fn size_hint(serializer: &$serializer, payload: &Self) -> Option<usize> {
                <&str>::size_hint(serializer, &payload.as_str())
            }
        }

        impl DeserializableWith<$serializer> for String {
            fn deserialize(serializer: &$serializer, mut bytes: &[u8]) -> ArconResult<Self> {
                let len = usize::deserialize_from(serializer, &mut bytes)?;
                if bytes.len() < len {
                    return arcon_err!("insufficient bytes to deserialize a String");
                }
                let vec = bytes[..len].to_vec();
                let res = String::from_utf8(vec)
                    .map_err(|e| arcon_err_kind!("String deserialization error: {}", e))?;
                Ok(res)
            }

            fn deserialize_from(
                serializer: &$serializer,
                mut source: impl Buf,
            ) -> ArconResult<Self> {
                let len = usize::deserialize_from(serializer, &mut source)?;
                if source.remaining() < len {
                    return arcon_err!("insufficient bytes to deserialize a String");
                }
                let mut vec = vec![0; len];
                source.copy_to_slice(&mut vec);
                let res = String::from_utf8(vec)
                    .map_err(|e| arcon_err_kind!("String deserialization error: {}", e))?;
                Ok(res)
            }
        }
    };
}
macro_rules! impl_byte_dump_for_vecs {
    ($serializer: ident) => {
        impl<T: SerializableWith<$serializer>> SerializableWith<$serializer> for &'_ [T] {
            fn serialize(serializer: &$serializer, payload: &Self) -> ArconResult<Vec<u8>> {
                let len = payload.len();
                let mut res = Vec::with_capacity(Self::size_hint(serializer, payload).unwrap_or(0));
                usize::serialize_into(serializer, &mut res, &len)?;
                for x in *payload {
                    T::serialize_into(serializer, &mut res, x)?;
                }
                Ok(res)
            }

            fn serialize_into(
                serializer: &$serializer,
                mut target: impl BufMut,
                payload: &Self,
            ) -> ArconResult<()> {
                let len = payload.len();
                usize::serialize_into(serializer, &mut target, &len)?;
                for x in *payload {
                    T::serialize_into(serializer, &mut target, x)?;
                }
                Ok(())
            }

            fn size_hint(serializer: &$serializer, payload: &Self) -> Option<usize> {
                let payload_size: usize = payload
                    .iter()
                    .map(|x| T::size_hint(serializer, x).unwrap_or(0))
                    .sum();
                Some(std::mem::size_of::<usize>() + payload_size)
            }
        }

        impl<T: SerializableWith<$serializer>> SerializableWith<$serializer> for Vec<T> {
            fn serialize(serializer: &$serializer, payload: &Self) -> ArconResult<Vec<u8>> {
                <&[T]>::serialize(serializer, &payload.as_slice())
            }

            fn serialize_into(
                serializer: &$serializer,
                target: impl BufMut,
                payload: &Self,
            ) -> ArconResult<()> {
                <&[T]>::serialize_into(serializer, target, &payload.as_slice())
            }

            fn size_hint(serializer: &$serializer, payload: &Self) -> Option<usize> {
                <&[T]>::size_hint(serializer, &payload.as_slice())
            }
        }

        impl<T: DeserializableWith<$serializer>> DeserializableWith<$serializer> for Vec<T> {
            fn deserialize(serializer: &$serializer, bytes: &[u8]) -> ArconResult<Self> {
                Self::deserialize_from(serializer, bytes)
            }

            fn deserialize_from(
                serializer: &$serializer,
                mut source: impl Buf,
            ) -> ArconResult<Self> {
                let len = usize::deserialize_from(serializer, &mut source)?;
                let cap = len * std::mem::size_of::<T>();
                if source.remaining() < cap {
                    return arcon_err!("insufficient bytes to deserialize a Vec");
                }
                let mut vec = Vec::with_capacity(cap);
                for _ in 0..len {
                    let x = T::deserialize_from(serializer, &mut source)?;
                    vec.push(x);
                }
                Ok(vec)
            }
        }
    };
}

macro_rules! impl_byte_dump_all {
    ($serializer: ident) => {
        // floats depend on unstable library features and don't make much sense as keys anyway

        impl_byte_dump!($serializer, to_ne_bytes, from_ne_bytes;
            u8, u16, u32, u64, usize, u128,
            i8, i16, i32, i64, isize, i128
            // ,f32, f64
        );
        impl_byte_dump_for_unit!($serializer);
        impl_byte_dump_for_tuples!($serializer;
            (A, B),
            (A, B, C),
            (A, B, C, D),
            (A, B, C, D, E),
            (A, B, C, D, E, F)
        );
        impl_byte_dump_for_node_id!($serializer);
        impl_byte_dump_for_string!($serializer);
        impl_byte_dump_for_vecs!($serializer);
    };
}

impl_byte_dump_all!(NativeEndianBytesDump);
impl_byte_dump_all!(BigEndianBytesDump);
impl_byte_dump_all!(LittleEndianBytesDump);

#[derive(Debug, Default, Copy, Clone)]
pub struct HashAndThen<S, H = BuildHasherDefault<DefaultHasher>>(S, H);
impl<T, S, H> SerializableWith<HashAndThen<S, H>> for T
where
    T: Hash,
    H: BuildHasher,
    u64: SerializableWith<S>,
{
    fn serialize(serializer: &HashAndThen<S, H>, payload: &Self) -> ArconResult<Vec<u8>> {
        let mut hasher = serializer.1.build_hasher();
        payload.hash(&mut hasher);
        let hashed = hasher.finish();

        <u64 as SerializableWith<S>>::serialize(&serializer.0, &hashed)
    }

    fn serialize_into(
        serializer: &HashAndThen<S, H>,
        target: impl BufMut,
        payload: &Self,
    ) -> ArconResult<()> {
        let mut hasher = serializer.1.build_hasher();
        payload.hash(&mut hasher);
        let hashed = hasher.finish();

        <u64 as SerializableWith<S>>::serialize_into(&serializer.0, target, &hashed)
    }

    fn size_hint(serializer: &HashAndThen<S, H>, _payload: &Self) -> Option<usize> {
        u64::size_hint(&serializer.0, &0)
    }
}

unsafe impl<T, S, H> SerializableFixedSizeWith<HashAndThen<S, H>> for T
where
    T: Hash,
    H: BuildHasher,
    u64: SerializableFixedSizeWith<S>,
{
    const SIZE: usize = <u64 as SerializableFixedSizeWith<S>>::SIZE;
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(feature = "arcon_serde")]
    #[test]
    fn test_primitives_fixed() {
        SerializableFixedSizeWith::serialize_check(&Bincode, &0u8).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0u16).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0u32).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0u64).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0usize).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0u128).unwrap();

        SerializableFixedSizeWith::serialize_check(&Bincode, &0i8).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0i16).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0i32).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0i64).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0isize).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0i128).unwrap();

        SerializableFixedSizeWith::serialize_check(&Bincode, &0f32).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &0f64).unwrap();

        SerializableFixedSizeWith::serialize_check(&Bincode, &'0').unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &false).unwrap();
        SerializableFixedSizeWith::serialize_check(&Bincode, &()).unwrap();
    }

    #[cfg(feature = "arcon_serde")]
    #[test]
    fn test_with_hashing() {
        SerializableFixedSizeWith::<HashAndThen<Bincode>>::serialize_check(
            &Default::default(),
            &"foobar",
        )
        .unwrap();

        SerializableFixedSizeWith::<HashAndThen<Bincode>>::serialize_check(&Default::default(), &[
            1, 2, 3, 4,
        ])
        .unwrap();
    }

    #[test]
    fn test_prost_serialization() {
        SerializableWith::serialize(&Prost, &"foobar".to_string()).unwrap();
        SerializableWith::serialize(&Prost, &vec![1, 2, 3]).unwrap();
    }

    #[cfg(feature = "arcon_serde")]
    #[test]
    fn test_bincode_strings() {
        let s = "foo".to_string();
        let serialized = SerializableWith::serialize(&Bincode, &s).unwrap();
        let deserialized: String = DeserializableWith::deserialize(&Bincode, &serialized).unwrap();
        assert_eq!(deserialized, s);
    }

    #[test]
    fn test_byte_dump() {
        type X = (u8, u64, i16);
        let payload = (255, 0xDEAD_BEEF_CAFE_BABE, -128);
        let bytes = X::serialize(&LittleEndianBytesDump, &payload).unwrap();
        assert_eq!(bytes, vec![
            255, 0xBE, 0xBA, 0xFE, 0xCA, 0xEF, 0xBE, 0xAD, 0xDE, 128, 255
        ]);
        let deserialized = X::deserialize(&LittleEndianBytesDump, &bytes).unwrap();
        assert_eq!(deserialized, payload);

        let str_payload = "123";
        let bytes = <&str>::serialize(&LittleEndianBytesDump, &str_payload).unwrap();
        assert_eq!(bytes, vec![3, 0, 0, 0, 0, 0, 0, 0, b'1', b'2', b'3']);
        let deserialized = String::deserialize(&LittleEndianBytesDump, &bytes).unwrap();
        assert_eq!(&deserialized, str_payload);

        let slice_payload = [1u8, 2, 3].as_ref();
        let bytes = <&[u8]>::serialize(&LittleEndianBytesDump, &slice_payload).unwrap();
        assert_eq!(bytes, vec![3, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3]);
        let deserialized = Vec::<u8>::deserialize(&LittleEndianBytesDump, &bytes).unwrap();
        assert_eq!(deserialized.as_slice(), slice_payload);
    }
}
