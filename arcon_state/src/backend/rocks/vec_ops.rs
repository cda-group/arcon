// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    data::{Metakey, Value},
    error::*,
    handles::BoxedIteratorOfResult,
    rocks::default_write_opts,
    serialization::{fixed_bytes, fixed_bytes::FixedBytes, protobuf},
    Handle, Rocks, VecOps, VecState,
};
use rocksdb::MergeOperands;
use std::{iter, mem};

impl VecOps for Rocks {
    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.remove(handle.id, &key)?;
        Ok(())
    }

    fn vec_append<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;

        let mut serialized = Vec::with_capacity(
            <usize as FixedBytes>::SIZE + protobuf::size_hint(&value).unwrap_or(0),
        );
        fixed_bytes::serialize_into(&mut serialized, &1usize)?;
        protobuf::serialize_into(&mut serialized, &value)?;

        let cf = self.get_cf_handle(handle.id)?;
        // See the vec_merge function in this module. It is set as the merge operator for every
        // vec state.
        Ok(self
            .db()
            .merge_cf_opt(cf, key, serialized, &default_write_opts())?)
    }

    fn vec_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<Vec<T>> {
        let key = handle.serialize_metakeys()?;
        if let Some(serialized) = self.get(handle.id, &key)? {
            // reader is updated to point at the yet unconsumed part of the serialized data
            let mut reader = &serialized[..];
            let len: usize = fixed_bytes::deserialize_from(&mut reader)?;
            let mut res = Vec::with_capacity(len);
            while !reader.is_empty() {
                let val = protobuf::deserialize_from(&mut reader)?;
                res.push(val);
            }
            // sanity check
            assert_eq!(res.len(), len);

            Ok(res)
        } else {
            Ok(vec![])
        }
    }

    fn vec_iter<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, T>> {
        let key = handle.serialize_metakeys()?;
        if let Some(serialized) = self.get(handle.id, &key)? {
            // TODO: this would be nicer with generators :(
            let mut reader = &serialized[..];
            let origin = reader.as_ptr() as usize;
            // we ignore the length, but we do the deserialization anyway to advance the reader
            let _: usize = fixed_bytes::deserialize_from(&mut reader)?;
            // this is safe because we're counting bytes
            let mut consumed = reader.as_ptr() as usize - origin;

            let iter = iter::from_fn(move || {
                // We cannot use the borrow from the outside, because it points to the local fn
                // variable. We have to force `serialized` to move into the closure. We have to
                // manually keep track of the number of consumed bytes, because otherwise we'd
                // have a self-referential struct.
                let mut reader = &serialized[consumed..];

                if !reader.is_empty() {
                    let res = protobuf::deserialize_from(&mut reader);
                    consumed = reader.as_ptr() as usize - origin;
                    Some(res)
                } else {
                    None
                }
            });

            Ok(Box::new(iter))
        } else {
            Ok(Box::new(iter::empty()))
        }
    }

    fn vec_set<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        value: Vec<T>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let raw_serialized_len: usize = value
            .iter()
            .flat_map(|x| protobuf::size_hint(x).into_iter())
            .sum();
        let cap = <usize as FixedBytes>::SIZE + raw_serialized_len;

        let mut storage = Vec::with_capacity(cap);
        fixed_bytes::serialize_into(&mut storage, &value.len())?;
        for elem in value {
            protobuf::serialize_into(&mut storage, &elem)?;
        }

        self.put(handle.id, key, storage)
    }

    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        values: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;

        // figuring out the correct capacity would require iterating through `values`, but we
        // cannot really consume the `values` iterator twice, so just preallocate a bunch of bytes
        let mut serialized = Vec::with_capacity(256);

        // reserve space for the length
        fixed_bytes::serialize_into(&mut serialized, &0usize)?;
        let mut len = 0usize;

        for elem in values {
            len += 1;
            protobuf::serialize_into(&mut serialized, &elem)?;
        }

        // fill in the length
        // BufMut impl for mutable slices starts at the beginning and shifts the slice, whereas the
        // impl for Vec starts at the end and extends it, so we want the first one
        fixed_bytes::serialize_into(&mut serialized.as_mut_slice(), &len)?;

        let cf = self.get_cf_handle(handle.id)?;
        self.db()
            .merge_cf_opt(cf, key, serialized, &default_write_opts())?;

        Ok(())
    }

    fn vec_len<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<usize> {
        let key = handle.serialize_metakeys()?;
        if let Some(storage) = self.get(handle.id, key)? {
            if storage.is_empty() {
                return Ok(0);
            }
            if storage.len() < <usize as FixedBytes>::SIZE {
                // this is certainly a bug, so let's not bother with a Result
                panic!("vec stored with partial size?");
            }

            let len = fixed_bytes::deserialize_from(&mut storage.as_ref())?;
            Ok(len)
        } else {
            Ok(0)
        }
    }

    fn vec_is_empty<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<bool> {
        Ok(self.vec_len(handle)? == 0)
    }
}

pub(crate) fn vec_merge(
    _key: &[u8],
    first: Option<&[u8]>,
    rest: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(
        mem::size_of::<usize>() + first.map(|x| x.len()).unwrap_or(0) + rest.size_hint().0,
    );

    // reserve space for the length
    fixed_bytes::serialize_into(&mut result, &0usize)
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
        fixed_bytes::deserialize_from(slice_ref)
            .or_else(|e| {
                // TODO: proper logging
                eprintln!("length deserialization error: {}", e);
                Err(())
            })
            .ok()
    }

    for mut op in first.into_iter().chain(rest) {
        len += get_len(&mut op)?;
        result.extend_from_slice(op);
    }

    // The second argument may seem weird, but look at the impl of BufMut for &mut [u8].
    // Just passing the result would actually _extend_ the vec, whereas we want to overwrite it
    // (the space was reserved at the beginning)
    fixed_bytes::serialize_into(&mut result.as_mut_slice(), &len)
        .or_else(|e| {
            // TODO: proper logging
            eprintln!("length serialization error: {}", e);
            Err(())
        })
        .ok()?;

    Some(result)
}
