// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{Metakey, Value},
    error::*,
    serialization::protobuf,
    sled::Sled,
    Handle, Reducer, ReducerOps, ReducerState,
};
use sled::MergeOperator;
use std::iter;

impl ReducerOps for Sled {
    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.remove(&handle.id, &key)?;
        Ok(())
    }

    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>> {
        let key = handle.serialize_metakeys()?;
        if let Some(storage) = self.get(&handle.id, &key)? {
            let value = protobuf::deserialize(&*storage)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn reducer_reduce<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let serialized = protobuf::serialize(&value)?;

        // See the make_reducer_merge function in this module. Its result is set as the merging
        // operator for this state.
        self.tree(&handle.id)?.merge(key, serialized)?;

        Ok(())
    }
}

pub fn make_reducer_merge<T, F>(reduce_fn: F) -> impl MergeOperator + 'static
where
    F: Reducer<T>,
    T: Value,
{
    move |_key: &[u8], existent: Option<&[u8]>, new: &[u8]| {
        let res = existent
            .into_iter()
            .chain(iter::once(new))
            .map(|bytes| protobuf::deserialize(bytes))
            .try_fold(None, |acc, value| -> Result<_> {
                match acc {
                    None => Ok(Some(value?)),
                    Some(old) => Ok(Some(reduce_fn(&old, &value?))),
                }
            });

        // TODO: change eprintlns to actual logs
        // we don't really have a way to send results back to rust across Sled ffi, so we just log the errors
        match res {
            Ok(Some(v)) => match protobuf::serialize(&v) {
                Ok(serialized) => Some(serialized),
                Err(e) => {
                    eprintln!("reduce state merge result serialization error: {}", e);
                    None
                }
            },
            Ok(None) => {
                eprintln!("reducing state merge result is None???");
                None
            }
            Err(e) => {
                eprintln!("reducing state merge error: {}", e);
                None
            }
        }
    }
}
