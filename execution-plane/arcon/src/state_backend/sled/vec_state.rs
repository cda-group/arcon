// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        serialization::{
            DeserializableWith, LittleEndianBytesDump, SerializableFixedSizeWith, SerializableWith,
        },
        sled::{Sled, StateCommon},
        state_types::{AppendingState, MergingState, State, VecState},
    },
};
use error::ResultExt;
use std::{iter, marker::PhantomData, mem};

pub struct SledVecState<IK, N, T, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, KS, TS> State<Sled, IK, N> for SledVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut Sled) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        backend.remove(&self.common.tree_name, &key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

// the fastest one possible on most architectures - we choose this instead of the native endian for
// portability
type VecLenSerializer = LittleEndianBytesDump;
const VEC_LEN_SERIALIZER: &VecLenSerializer = &LittleEndianBytesDump;

// NOTE: this requires all of the operands to begin with an LE-encoded usize length
pub fn vec_merge(_key: &[u8], existent: Option<&[u8]>, new: &[u8]) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(
        mem::size_of::<usize>() + existent.map(|x| x.len()).unwrap_or(0) + new.len(),
    );

    // reserve space for the length
    usize::serialize_into(VEC_LEN_SERIALIZER, &mut result, &0)
        .or_else(|e| {
            // TODO: proper logging
            eprintln!("length serialization error: {}", e);
            Err(())
        })
        .ok()?;
    let mut len = 0usize;

    // Utility to consume the first few bytes from the slice and interpret that as length. The
    // passed slice will get shifted so it points right after the length bytes.
    fn get_len(slice_ref: &mut &[u8]) -> Option<usize> {
        usize::deserialize_from(VEC_LEN_SERIALIZER, slice_ref)
            .or_else(|e| {
                // TODO: proper logging
                eprintln!("length deserialization error: {}", e);
                Err(())
            })
            .ok()
    }

    for mut op in existent.into_iter().chain(iter::once(new)) {
        len += get_len(&mut op)?;
        result.extend_from_slice(op);
    }

    // The second argument may seem weird, but look at the impl of BufMut for &mut [u8].
    // Just passing the result would actually _extend_ the vec, whereas we want to overwrite it
    // (the space was reserved at the beginning)
    usize::serialize_into(VEC_LEN_SERIALIZER, result.as_mut_slice(), &len)
        .or_else(|e| {
            // TODO: proper logging
            eprintln!("length serialization error: {}", e);
            Err(())
        })
        .ok()?;

    Some(result)
}

impl<IK, N, T, KS, TS> AppendingState<Sled, IK, N, T, Vec<T>> for SledVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn get(&self, backend: &Sled) -> ArconResult<Vec<T>> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(serialized) = backend.get(&self.common.tree_name, &key)? {
            // reader is updated to point at the yet unconsumed part of the serialized data
            let mut reader = &serialized[..];
            let len = usize::deserialize_from(VEC_LEN_SERIALIZER, &mut reader)?;
            let mut res = Vec::with_capacity(len);
            while !reader.is_empty() {
                let val = T::deserialize_from(&self.common.value_serializer, &mut reader)?;
                res.push(val);
            }
            // sanity check
            assert_eq!(res.len(), len);

            Ok(res)
        } else {
            Ok(vec![])
        }
    }

    fn append(&self, backend: &mut Sled, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;

        let mut serialized = Vec::with_capacity(
            mem::size_of::<usize>()
                + T::size_hint(&self.common.value_serializer, &value).unwrap_or(0),
        );
        usize::serialize_into(VEC_LEN_SERIALIZER, &mut serialized, &1)?;
        T::serialize_into(&self.common.value_serializer, &mut serialized, &value)?;

        let tree = backend.tree(&self.common.tree_name)?;
        // See the vec_merge function in this module. It is set as the merge operator for every vec state.
        tree.merge(key, serialized)
            .ctx("Could not perform merge operation")?;

        Ok(())
    }
}

impl<IK, N, T, KS, TS> MergingState<Sled, IK, N, T, Vec<T>> for SledVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
}

impl<IK, N, T, KS, TS> VecState<Sled, IK, N, T> for SledVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn set(&self, backend: &mut Sled, value: Vec<T>) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let raw_serialized_len: usize = value
            .iter()
            .flat_map(|x| T::size_hint(&self.common.value_serializer, x).into_iter())
            .sum();
        let cap = mem::size_of::<usize>() + raw_serialized_len;

        let mut storage = Vec::with_capacity(cap);
        usize::serialize_into(VEC_LEN_SERIALIZER, &mut storage, &value.len())?;
        for elem in value {
            T::serialize_into(&self.common.value_serializer, &mut storage, &elem)?;
        }

        backend.put(&self.common.tree_name, &key, &storage)?;

        Ok(())
    }

    fn add_all(&self, backend: &mut Sled, values: impl IntoIterator<Item = T>) -> ArconResult<()>
    where
        Self: Sized,
    {
        let key = self.common.get_db_key_prefix()?;

        // figuring out the correct capacity would require iterating through `values`, but
        // we cannot really consume the `values` iterator twice, so just preallocate a bunch of bytes
        let mut serialized = Vec::with_capacity(256);

        // reserve space for the length
        usize::serialize_into(VEC_LEN_SERIALIZER, &mut serialized, &0)?;
        let mut len = 0usize;

        for elem in values {
            len += 1;
            T::serialize_into(&self.common.value_serializer, &mut serialized, &elem)?;
        }

        // fill in the length
        // BufMut impl for mutable slices starts at the beginning and shifts the slice, whereas the
        // impl for Vec starts at the end and extends it, so we want the first one
        usize::serialize_into(VEC_LEN_SERIALIZER, serialized.as_mut_slice(), &len)?;

        backend
            .tree(&self.common.tree_name)?
            .merge(key, serialized)
            .ctx("Could not execute merge operation")?;

        Ok(())
    }

    fn add_all_dyn(
        &self,
        backend: &mut Sled,
        values: &mut dyn Iterator<Item = T>,
    ) -> ArconResult<()> {
        self.add_all(backend, values)
    }

    fn is_empty(&self, backend: &Sled) -> ArconResult<bool> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(storage) = backend.get(&self.common.tree_name, &key)? {
            if storage.is_empty() {
                return Ok(true);
            }
            if storage.len() < mem::size_of::<usize>() {
                return arcon_err!("stored vec with partial size?");
            }

            let len = usize::deserialize_from(VEC_LEN_SERIALIZER, &mut storage.as_ref())?;
            Ok(len == 0)
        } else {
            Ok(true)
        }
    }

    fn len(&self, backend: &Sled) -> ArconResult<usize> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(storage) = backend.get(&self.common.tree_name, &key)? {
            if storage.is_empty() {
                return Ok(0);
            }
            if storage.len() < mem::size_of::<usize>() {
                return arcon_err!("stored vec with partial size?");
            }

            let len = usize::deserialize_from(VEC_LEN_SERIALIZER, &mut storage.as_ref())?;
            Ok(len)
        } else {
            Ok(0)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        serialization::NativeEndianBytesDump, sled::test::TestDb, VecStateBuilder,
    };

    #[test]
    fn vec_state_test() {
        let mut db = TestDb::new();
        let vec_state = db.new_vec_state(
            "test_state",
            (),
            (),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );
        assert!(vec_state.is_empty(&db).unwrap());
        assert_eq!(vec_state.len(&db).unwrap(), 0);

        vec_state.append(&mut db, 1).unwrap();
        vec_state.append(&mut db, 2).unwrap();
        vec_state.append(&mut db, 3).unwrap();
        vec_state.add_all(&mut db, vec![4, 5, 6]).unwrap();

        assert_eq!(vec_state.get(&db).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(vec_state.len(&db).unwrap(), 6);
    }
}
