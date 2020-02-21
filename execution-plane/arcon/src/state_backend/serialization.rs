// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::ArconResult;
use bytes::{buf::ext::*, Buf, BufMut};
use prost::Message;
use serde::{Deserialize, Serialize};
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

macro_rules! impl_serializable_fixed_size {
    ($serialization_strategy: ty; $($t: ty : $size: expr),+) => {
        $(unsafe impl SerializableFixedSizeWith<$serialization_strategy> for $t {
            const SIZE: usize = $size;
        })+
    };
}

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

impl_serializable_fixed_size!(Bincode;
    u8: 1, u16: 2, u32: 4, u64: 8, usize: std::mem::size_of::<usize>(), u128: 16,
    i8: 1, i16: 2, i32: 4, i64: 8, isize: std::mem::size_of::<isize>(), i128: 16,
    f32: 4, f64: 8,
    char: 1, bool: 1, (): 0
);

#[derive(Debug, Default, Copy, Clone)]
pub struct Prost;
impl<T> SerializableWith<Prost> for T
where
    T: Message,
{
    fn serialize(_serializer: &Prost, payload: &Self) -> ArconResult<Vec<u8>> {
        let size = payload.encoded_len();
        let mut buf = vec![0u8; size];
        payload.encode(&mut buf).map_err(|e| {
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
        payload.encode(&mut target).map_err(|e| {
            arcon_err_kind!(
                "Could not serialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }

    fn size_hint(_serializer: &Prost, payload: &Self) -> Option<usize> {
        Some(payload.encoded_len())
    }
}

impl<T> DeserializableWith<Prost> for T
where
    T: Message + Default,
{
    fn deserialize(_serializer: &Prost, bytes: &[u8]) -> ArconResult<Self> {
        <T as Message>::decode(bytes).map_err(|e| {
            arcon_err_kind!(
                "Could not deserialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }

    fn deserialize_from(_serializer: &Prost, source: impl Buf) -> ArconResult<Self> {
        <T as Message>::decode(source).map_err(|e| {
            arcon_err_kind!(
                "Could not deserialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }
}
// even integers have variable length encodings with protobuf, so we cannot implement SerializableFixedSizeWith for anything

// similar to bincode, but only for numeric primitives and with specified endianness
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

// floats depend on unstable library features and don't make much sense as keys anyway
// (this is mostly for serializing keys btw)

impl_byte_dump!(NativeEndianBytesDump, to_ne_bytes, from_ne_bytes;
    u8, u16, u32, u64, usize, u128,
    i8, i16, i32, i64, isize, i128
//    ,f32, f64
);

impl_byte_dump!(BigEndianBytesDump, to_be_bytes, from_be_bytes;
    u8, u16, u32, u64, usize, u128,
    i8, i16, i32, i64, isize, i128
//    ,f32, f64
);

impl_byte_dump!(LittleEndianBytesDump, to_le_bytes, from_le_bytes;
    u8, u16, u32, u64, usize, u128,
    i8, i16, i32, i64, isize, i128
//    ,f32, f64
);

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

        assert_eq!(
            <u32 as DeserializableWith<Prost>>::deserialize(&Prost, &[]).unwrap(),
            0u32
        );
    }

    #[test]
    fn test_bincode_strings() {
        let s = "foo".to_string();
        let serialized = SerializableWith::serialize(&Bincode, &s).unwrap();
        let deserialized: String = DeserializableWith::deserialize(&Bincode, &serialized).unwrap();
        assert_eq!(deserialized, s);
    }
}
