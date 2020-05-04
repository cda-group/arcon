// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
// TODO: may want to investigate hooking up to arcon allocator (all the Vec::with_capacity)

pub mod protobuf {
    use crate::error::*;
    use bytes::{Buf, BufMut};
    use prost::{encoding::encoded_len_varint, Message};

    pub fn serialize(payload: &impl Message) -> Result<Vec<u8>> {
        let size = size_hint(payload).unwrap_or(0);
        let mut buf = Vec::with_capacity(size);
        payload.encode_length_delimited(&mut buf)?;
        Ok(buf)
    }

    pub fn serialize_into(mut target: impl BufMut, payload: &impl Message) -> Result<()> {
        Ok(payload.encode_length_delimited(&mut target)?)
    }

    pub fn deserialize<T: Message + Default>(bytes: &[u8]) -> Result<T> {
        Ok(T::decode_length_delimited(bytes)?)
    }

    pub fn deserialize_from<T: Message + Default>(source: impl Buf) -> Result<T> {
        Ok(T::decode_length_delimited(source)?)
    }

    pub fn size_hint(payload: &impl Message) -> Option<usize> {
        let len = payload.encoded_len();
        Some(len + encoded_len_varint(len as u64))
    }
}

pub mod fixed_bytes {
    use crate::error::*;
    use bytes::{Buf, BufMut};
    use std::mem::MaybeUninit;

    pub trait FixedBytes: Sized {
        const SIZE: usize;

        fn serialize(payload: &Self) -> Result<Vec<u8>>;
        fn serialize_into(target: impl BufMut, payload: &Self) -> Result<()>;
        fn deserialize(bytes: &[u8]) -> Result<Self>;
        fn deserialize_from(source: impl Buf) -> Result<Self>;
        fn serialize_check(payload: &Self) -> Result<Vec<u8>> {
            let serialized = Self::serialize(payload)?;
            assert_eq!(
                serialized.len(),
                Self::SIZE,
                "type: {}",
                std::any::type_name::<Self>()
            );
            Ok(serialized)
        }
    }

    pub fn serialize<T: FixedBytes>(payload: &T) -> Result<Vec<u8>> {
        T::serialize(payload)
    }
    pub fn serialize_into<T: FixedBytes>(target: impl BufMut, payload: &T) -> Result<()> {
        T::serialize_into(target, payload)
    }
    pub fn deserialize<T: FixedBytes>(bytes: &[u8]) -> Result<T> {
        T::deserialize(bytes)
    }
    pub fn deserialize_from<T: FixedBytes>(source: impl Buf) -> Result<T> {
        T::deserialize_from(source)
    }
    pub fn serialize_check<T: FixedBytes>(payload: &T) -> Result<Vec<u8>> {
        T::serialize_check(payload)
    }

    macro_rules! impl_fixed_bytes {
        ($($t: ty),+) => {$(
            impl FixedBytes for $t {
                const SIZE: usize = std::mem::size_of::<Self>();

                fn serialize(payload: &Self) -> Result<Vec<u8>> {
                    let bytes = payload.to_le_bytes();
                    Ok(bytes.to_vec())
                }

                fn serialize_into(
                    mut target: impl BufMut,
                    payload: &Self,
                ) -> Result<()> {
                    let bytes = payload.to_le_bytes();
                    let needed = bytes.len();
                    let dest_len = target.remaining_mut();
                    ensure!(dest_len >= needed, FixedBytesSerializationError { needed, dest_len });
                    target.put_slice(&bytes);
                    Ok(())
                }

                fn deserialize(bytes: &[u8]) -> Result<Self> {
                    let mut buf = [0; std::mem::size_of::<Self>()];
                    let needed = buf.len();
                    let source_len = bytes.len();
                    ensure!(source_len >= needed,
                        FixedBytesDeserializationError { needed, source_len });
                    buf.copy_from_slice(bytes);
                    Ok(Self::from_le_bytes(buf))
                }

                fn deserialize_from(mut source: impl Buf) -> Result<Self> {
                    let mut buf = [0; std::mem::size_of::<Self>()];
                    let needed = buf.len();
                    let source_len = source.remaining();
                    ensure!(source_len >= needed,
                        FixedBytesDeserializationError { needed, source_len });
                    source.copy_to_slice(&mut buf);
                    Ok(Self::from_le_bytes(buf))
                }
            }
        )+};
    }
    impl_fixed_bytes!(u8, u16, u32, u64, usize, u128, i8, i16, i32, i64, isize, i128, f32, f64);

    macro_rules! impl_fixed_bytes_for_tuples {
        ($(($($T: ident),*)),*) => {$(
            #[allow(non_snake_case)]
            impl<$($T: FixedBytes),*> FixedBytes for ($($T),*) {
                const SIZE: usize = 0 $(+ $T::SIZE)*;

                fn serialize(payload: &Self) -> Result<Vec<u8>> {
                    let size = 0 $(+ std::mem::size_of::<$T>())*;
                    let mut res = Vec::with_capacity(size);
                    let ($($T),*) = payload;
                    $(
                        $T::serialize_into(&mut res, $T)?;
                    )*
                    Ok(res)
                }

                fn serialize_into(
                    mut target: impl BufMut,
                    payload: &Self,
                ) -> Result<()> {
                    let ($($T),*) = payload;
                    $(
                        $T::serialize_into(&mut target, $T)?;
                    )*
                    Ok(())
                }

                fn deserialize(bytes: &[u8]) -> Result<Self> {
                    Self::deserialize_from(bytes)
                }

                fn deserialize_from(mut source: impl Buf) -> Result<Self> {
                    Ok(($(
                        $T::deserialize_from(&mut source)?
                    ),*))
                }
            }
        )*};
    }
    impl_fixed_bytes_for_tuples!(
        (A, B),
        (A, B, C),
        (A, B, C, D),
        (A, B, C, D, E),
        (A, B, C, D, E, F)
    );

    impl FixedBytes for () {
        const SIZE: usize = 0;

        fn serialize(_payload: &Self) -> Result<Vec<u8>> {
            Ok(vec![])
        }

        fn serialize_into(_target: impl BufMut, _payload: &Self) -> Result<()> {
            Ok(())
        }

        fn deserialize(_bytes: &[u8]) -> Result<Self> {
            Ok(())
        }

        fn deserialize_from(_source: impl Buf) -> Result<Self> {
            Ok(())
        }
    }

    impl FixedBytes for bool {
        const SIZE: usize = 1;

        fn serialize(payload: &Self) -> Result<Vec<u8>> {
            u8::serialize(&(if *payload { 1 } else { 0 }))
        }

        fn serialize_into(target: impl BufMut, payload: &Self) -> Result<()> {
            u8::serialize_into(target, &(if *payload { 1 } else { 0 }))
        }

        fn deserialize(bytes: &[u8]) -> Result<Self> {
            Ok(u8::deserialize(bytes)? != 0)
        }

        fn deserialize_from(source: impl Buf) -> Result<Self> {
            Ok(u8::deserialize_from(source)? != 0)
        }
    }

    impl<T: FixedBytes, const N: usize> FixedBytes for [T; N] {
        const SIZE: usize = T::SIZE * N;

        fn serialize(payload: &Self) -> Result<Vec<u8>> {
            let mut buf = Vec::with_capacity(Self::SIZE);
            for x in payload.iter() {
                T::serialize_into(&mut buf, x)?;
            }
            Ok(buf)
        }

        fn serialize_into(mut target: impl BufMut, payload: &Self) -> Result<()> {
            for x in payload.iter() {
                T::serialize_into(&mut target, x)?;
            }
            Ok(())
        }

        fn deserialize(bytes: &[u8]) -> Result<Self> {
            Self::deserialize_from(bytes)
        }

        fn deserialize_from(mut source: impl Buf) -> Result<Self> {
            let mut result: [MaybeUninit<T>; N] = unsafe { MaybeUninit::uninit().assume_init() };
            for elem in &mut result[..] {
                *elem = MaybeUninit::new(T::deserialize_from(&mut source)?);
            }
            // transmute doesn't work here. I hope this gets fixed someday :/
            // Ok(unsafe { std::mem::transmute(result) })

            // in the meantime we can do this and hope that LLVM sees through this bullshit
            // and doesn't use twice the stack space it needs
            let typed_result = unsafe { std::ptr::read(&result as *const _ as *const [T; N]) };
            std::mem::forget(result);
            Ok(typed_result)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::error::*;
    use std::string::ToString;

    #[test]
    fn test_primitives_fixed() {
        fixed_bytes::serialize_check(&0u8).unwrap();
        fixed_bytes::serialize_check(&0u16).unwrap();
        fixed_bytes::serialize_check(&0u32).unwrap();
        fixed_bytes::serialize_check(&0u64).unwrap();
        fixed_bytes::serialize_check(&0usize).unwrap();
        fixed_bytes::serialize_check(&0u128).unwrap();

        fixed_bytes::serialize_check(&0i8).unwrap();
        fixed_bytes::serialize_check(&0i16).unwrap();
        fixed_bytes::serialize_check(&0i32).unwrap();
        fixed_bytes::serialize_check(&0i64).unwrap();
        fixed_bytes::serialize_check(&0isize).unwrap();
        fixed_bytes::serialize_check(&0i128).unwrap();

        fixed_bytes::serialize_check(&0f32).unwrap();
        fixed_bytes::serialize_check(&0f64).unwrap();

        fixed_bytes::serialize_check(&false).unwrap();
        fixed_bytes::serialize_check(&()).unwrap();
    }

    #[test]
    fn test_protobuf_serialization() {
        protobuf::serialize(&0u32).unwrap();
        protobuf::serialize(&0u64).unwrap();

        protobuf::serialize(&0i32).unwrap();
        protobuf::serialize(&0i64).unwrap();

        protobuf::serialize(&0f32).unwrap();
        protobuf::serialize(&0f64).unwrap();

        protobuf::serialize(&()).unwrap();

        protobuf::serialize(&"foobar".to_string()).unwrap();
        protobuf::serialize(&vec![1, 2, 3]).unwrap();
    }

    #[test]
    fn test_fixed_bytes() {
        type X = (u8, u64, i16);
        let payload = (255, 0xDEAD_BEEF_CAFE_BABE, -128);
        let bytes = fixed_bytes::serialize(&payload).unwrap();
        assert_eq!(bytes, vec![
            255, 0xBE, 0xBA, 0xFE, 0xCA, 0xEF, 0xBE, 0xAD, 0xDE, 128, 255
        ]);
        let deserialized = fixed_bytes::deserialize::<X>(&bytes).unwrap();
        assert_eq!(deserialized, payload);

        let payload: [u8; 3] = [1u8, 2, 3];
        let bytes = fixed_bytes::serialize(&payload).unwrap();
        assert_eq!(bytes, vec![1, 2, 3]);
        let deserialized = fixed_bytes::deserialize::<[u8; 3]>(&bytes).unwrap();
        assert_eq!(deserialized, payload);
    }

    #[test]
    fn test_errors() {
        type X = (u8, u64, i16);
        let too_short = &[42u8][..];
        let deserialized = fixed_bytes::deserialize::<X>(too_short);
        let err = deserialized.err().unwrap();
        println!("{}", err);
        if let Some(backtrace) = err.backtrace() {
            println!("{:?}", backtrace);
        }

        let deserialized = protobuf::deserialize::<u64>(too_short);
        let err = deserialized.err().unwrap();
        println!("{}", err);
        if let Some(backtrace) = err.backtrace() {
            println!("{:?}", backtrace);
        }
    }
}
