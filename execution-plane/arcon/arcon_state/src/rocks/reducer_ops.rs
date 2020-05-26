// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, serialization::protobuf, Handle, Metakey, Reducer, ReducerOps, ReducerState, Rocks,
    Value,
};
use rocksdb::{merge_operator::MergeFn, MergeOperands};

impl ReducerOps for Rocks {
    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.remove(handle.id, &key)?;
        Ok(())
    }

    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>> {
        let key = handle.serialize_metakeys()?;
        if let Some(storage) = self.get(handle.id, &key)? {
            let value = protobuf::deserialize(&*storage)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn reducer_reduce<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
        value: T,
    ) -> Result<()> {
        let backend = self.initialized_mut()?;

        let key = handle.serialize_metakeys()?;
        let serialized = protobuf::serialize(&value)?;

        let cf = backend.get_cf_handle(handle.id)?;
        // See the make_reducer_merge function in this module. Its result is set as the merging
        // operator for this state.
        Ok(backend.db.merge_cf(cf, key, serialized)?)
    }
}

pub fn make_reducer_merge<T, F>(reduce_fn: F) -> impl MergeFn + Clone
where
    F: Reducer<T>,
    T: Value,
{
    move |_key: &[u8], first: Option<&[u8]>, rest: &mut MergeOperands| {
        let res: Result<Option<T>> = first
            .into_iter()
            .chain(rest)
            .map(|bytes| protobuf::deserialize::<T>(bytes))
            .try_fold(None, |acc, value| match acc {
                None => Ok(Some(value?)),
                Some(old) => Ok(Some(reduce_fn(&old, &value?))),
            });

        // TODO: change eprintlns to actual logs
        // we don't really have a way to send results back to rust across rocksdb ffi, so we just
        // log the errors
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
