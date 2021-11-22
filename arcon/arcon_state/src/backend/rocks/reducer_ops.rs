#[cfg(feature = "metrics")]
use crate::metrics_utils::*;
use crate::{
    data::{Metakey, Value},
    error::*,
    rocks::default_write_opts,
    serialization::protobuf,
    Handle, Reducer, ReducerOps, ReducerState, Rocks,
};

use rocksdb::{merge_operator::MergeFn, MergeOperands};

impl ReducerOps for Rocks {
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
            #[cfg(feature = "metrics")]
            record_bytes_read(handle.name(), storage.len() as u64, self.name.as_str());
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

        let cf = self.get_cf_handle(&handle.id)?;
        // See the make_reducer_merge function in this module. Its result is set as the merging
        // operator for this state.
        #[cfg(feature = "metrics")]
        record_bytes_written(handle.name(), serialized.len() as u64, self.name.as_str());
        Ok(self
            .db()
            .merge_cf_opt(cf, key, serialized, &default_write_opts())?)
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
