use crate::prelude::ArconResult;
use bytes::{buf::ext::*, Buf, BufMut};
use prost::Message;
use serde::{export::PhantomData, Deserialize, Serialize};
use std::{
    any::type_name,
    collections::hash_map::DefaultHasher,
    hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
};

trait SerializableWith<S>: Sized {
    type Serializer;

    fn serialize(serializer: &Self::Serializer, payload: &Self) -> ArconResult<Vec<u8>>;
    fn serialize_into(
        serializer: &Self::Serializer,
        target: impl BufMut,
        payload: &Self,
    ) -> ArconResult<()>;
}

trait DeserializableWith<S>: Sized {
    type Serializer;

    fn deserialize(serializer: &Self::Serializer, bytes: &[u8]) -> ArconResult<Self>;
    fn deserialize_from(serializer: &Self::Serializer, source: impl Buf) -> ArconResult<Self>;
}

/// unsafe: implementations must assure that Self always serializes to buffers of size Self::SIZE
unsafe trait SerializableFixedSizeWith<S>: SerializableWith<S> {
    const SIZE: usize;
    fn serialize_check(serializer: &Self::Serializer, payload: &Self) -> ArconResult<Vec<u8>> {
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

struct Bincode;
impl<T> SerializableWith<Bincode> for T
where
    T: Serialize,
{
    type Serializer = ();

    fn serialize(_serializer: &Self::Serializer, payload: &Self) -> ArconResult<Vec<u8>> {
        bincode::serialize(payload).map_err(|e| {
            arcon_err_kind!(
                "Could not serialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }

    fn serialize_into(
        _serializer: &Self::Serializer,
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
}

impl<T> DeserializableWith<Bincode> for T
where
    T: for<'de> Deserialize<'de>,
{
    type Serializer = ();

    fn deserialize(_serializer: &Self::Serializer, bytes: &[u8]) -> ArconResult<Self> {
        bincode::deserialize(bytes).map_err(|e| {
            arcon_err_kind!(
                "Could not deserialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }

    fn deserialize_from(_serializer: &Self::Serializer, source: impl Buf) -> ArconResult<Self> {
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
    f32: 4, f64: 4,
    char: 1, bool: 1, (): 0
);

struct Prost;
impl<T> SerializableWith<Prost> for T
where
    T: Message,
{
    type Serializer = ();

    fn serialize(_serializer: &Self::Serializer, payload: &Self) -> ArconResult<Vec<u8>> {
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
        _serializer: &Self::Serializer,
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
}

impl<T> DeserializableWith<Prost> for T
where
    T: Message + Default,
{
    type Serializer = ();

    fn deserialize(_serializer: &Self::Serializer, bytes: &[u8]) -> ArconResult<Self> {
        <T as Message>::decode(bytes).map_err(|e| {
            arcon_err_kind!(
                "Could not deserialize payload of type `{}`: {}",
                type_name::<Self>(),
                e
            )
        })
    }

    fn deserialize_from(_serializer: &Self::Serializer, source: impl Buf) -> ArconResult<Self> {
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
struct NativeEndianBytesDump;
struct BigEndianBytesDump;
struct LittleEndianBytesDump;

macro_rules! impl_byte_dump {
    ($serializer: ty, $to_bytes: ident, $from_bytes: ident; $($t: ty),+) => {$(
        impl SerializableWith<$serializer> for $t {
            type Serializer = ();

            fn serialize(_serializer: &Self::Serializer, payload: &Self) -> ArconResult<Vec<u8>> {
                let bytes = payload.$to_bytes();
                let mut buf = vec![0; bytes.len()];
                buf.copy_from_slice(&bytes);
                Ok(buf)
            }

            fn serialize_into(
                _serializer: &Self::Serializer,
                mut target: impl BufMut,
                payload: &Self,
            ) -> ArconResult<()> {
                let bytes = payload.$to_bytes();
                target.put_slice(&bytes);
                Ok(())
            }
        }

        impl DeserializableWith<$serializer> for $t {
            type Serializer = ();

            fn deserialize(_serializer: &Self::Serializer, bytes: &[u8]) -> ArconResult<Self> {
                let mut buf = [0; std::mem::size_of::<Self>()];
                buf.copy_from_slice(bytes);
                Ok(Self::$from_bytes(buf))
            }

            fn deserialize_from(_serializer: &Self::Serializer, mut source: impl Buf) -> ArconResult<Self> {
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

struct HashAndThen<S, H = BuildHasherDefault<DefaultHasher>>(PhantomData<(S, H)>);
impl<T, S, H> SerializableWith<HashAndThen<S, H>> for T
where
    T: Hash,
    H: BuildHasher,
    u64: SerializableWith<S>,
{
    type Serializer = (H, <u64 as SerializableWith<S>>::Serializer);

    fn serialize(serializer: &Self::Serializer, payload: &Self) -> ArconResult<Vec<u8>> {
        let mut hasher = serializer.0.build_hasher();
        payload.hash(&mut hasher);
        let hashed = hasher.finish();

        <u64 as SerializableWith<S>>::serialize(&serializer.1, &hashed)
    }

    fn serialize_into(
        serializer: &Self::Serializer,
        target: impl BufMut,
        payload: &Self,
    ) -> ArconResult<()> {
        let mut hasher = serializer.0.build_hasher();
        payload.hash(&mut hasher);
        let hashed = hasher.finish();

        <u64 as SerializableWith<S>>::serialize_into(&serializer.1, target, &hashed)
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
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0u8).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0u16).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0u32).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0u64).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0usize).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0u128).unwrap();

        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0i8).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0i16).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0i32).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0i64).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0isize).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0i128).unwrap();

        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0f32).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &0f64).unwrap();

        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &'0').unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &false).unwrap();
        SerializableFixedSizeWith::<Bincode>::serialize_check(&(), &()).unwrap();
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
        SerializableWith::<Prost>::serialize(&(), &"foobar".to_string()).unwrap();
        SerializableWith::<Prost>::serialize(&(), &vec![1, 2, 3]).unwrap();

        assert_eq!(
            <u32 as DeserializableWith<Prost>>::deserialize(&(), &[]).unwrap(),
            0u32
        );
    }
}
