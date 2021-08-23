// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    data::{Metakey, Value},
    error::*,
    handles::BoxedIteratorOfResult,
    serialization::{fixed_bytes, fixed_bytes::FixedBytes, protobuf},
    sled::Sled,
    Handle, VecOps, VecState,
};

#[cfg(feature = "metrics")]
use crate::metrics_utils::*;

use std::iter;

impl VecOps for Sled {
    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.remove(&handle.id, &key)?;
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
        #[cfg(feature = "metrics")]
        record_bytes_written(handle.name(), serialized.len() as u64, self.name.as_str());

        let tree = self.tree(&handle.id)?;
        // See the vec_merge function in this module. It is set as the merge operator for every vec state.
        tree.merge(key, serialized)?;

        Ok(())
    }

    fn vec_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<Vec<T>> {
        let key = handle.serialize_metakeys()?;
        if let Some(serialized) = self.get(&handle.id, &key)? {
            // reader is updated to point at the yet unconsumed part of the serialized data
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), serialized.len() as u64, self.name.as_str());
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
        if let Some(serialized) = self.get(&handle.id, &key)? {
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
                // Also, we have to recalculate the origin, because the `serialized` is an `IVec`,
                // so the actual contents can be inline, so they can move in memory between
                // iterations!
                let origin = serialized.as_ref().as_ptr() as usize;
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
        #[cfg(feature = "metrics")]
        record_bytes_written(handle.name(), storage.len() as u64, self.name.as_str());
        self.put(&handle.id, &key, &storage)?;

        Ok(())
    }

    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        values: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;

        // figuring out the correct capacity would require iterating through `values`, but
        // we cannot really consume the `values` iterator twice, so just preallocate a bunch of bytes
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

        self.tree(&handle.id)?.merge(key, serialized)?;

        Ok(())
    }

    fn vec_len<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<usize> {
        let key = handle.serialize_metakeys()?;
        if let Some(storage) = self.get(&handle.id, &key)? {
            if storage.is_empty() {
                return Ok(0);
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

pub fn vec_merge(_key: &[u8], existent: Option<&[u8]>, new: &[u8]) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(
        <usize as FixedBytes>::SIZE + existent.map(|x| x.len()).unwrap_or(0) + new.len(),
    );

    // reserve space for the length
    if let Err(e) = fixed_bytes::serialize_into(&mut result, &0usize) {
        eprintln!("length serialization error: {}", e);
        return None;
    }

    let mut len = 0usize;

    // Utility to consume the first few bytes from the slice and interpret that as length. The
    // passed slice will get shifted so it points right after the length bytes.
    fn get_len(slice_ref: &mut &[u8]) -> Option<usize> {
        fixed_bytes::deserialize_from(slice_ref)
            .map_err(|e| {
                eprintln!("length deserialization error: {}", e);
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
    if let Err(e) = fixed_bytes::serialize_into(&mut result.as_mut_slice(), &len) {
        eprintln!("length serialization error: {}", e);
        return None;
    }

    Some(result)
}
