// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        sled::{Sled, StateCommon},
        state_types::{AppendingState, MergingState, ReducingState, State},
    },
};
use error::ResultExt;
use itertools::Itertools;
use sled::MergeOperator;
use std::{iter, marker::PhantomData};

pub struct SledReducingState<IK, N, T, F, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,

    // here mostly for debugging, we may fall back to it in the future if Sled merge proves unreliable
    #[allow(unused)]
    pub(crate) reduce_fn: F,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, F, KS, TS> State<Sled, IK, N> for SledReducingState<IK, N, T, F, KS, TS>
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

pub fn make_reducing_merge<T, F, TS>(
    reduce_fn: F,
    value_serializer: TS,
) -> impl MergeOperator + 'static
where
    F: Fn(&T, &T) -> T + Send + Sync + 'static,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    TS: Send + Sync + 'static,
{
    move |_key: &[u8], existant: Option<&[u8]>, new: &[u8]| {
        let res = existant
            .into_iter()
            .chain(iter::once(new))
            .map(|bytes| T::deserialize(&value_serializer, bytes))
            .fold_results(None, |acc, value| match acc {
                None => Some(value),
                Some(old) => Some(reduce_fn(&old, &value)),
            });

        // TODO: change eprintlns to actual logs
        // we don't really have a way to send results back to rust across Sled ffi, so we just log the errors
        match res {
            Ok(Some(v)) => match T::serialize(&value_serializer, &v) {
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

impl<IK, N, T, F, KS, TS> AppendingState<Sled, IK, N, T, Option<T>>
    for SledReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
    fn get(&self, backend: &Sled) -> ArconResult<Option<T>> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(storage) = backend.get(&self.common.tree_name, &key)? {
            let value = T::deserialize(&self.common.value_serializer, &*storage)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn append(&self, backend: &mut Sled, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let serialized = T::serialize(&self.common.value_serializer, &value)?;

        // See the make_reducing_merge function in this module. Its result is set as the merging operator for this state.
        backend
            .tree(&self.common.tree_name)?
            .merge(key, serialized)
            .ctx("Could not perform merge operation")?;

        Ok(())
    }
}

impl<IK, N, T, F, KS, TS> MergingState<Sled, IK, N, T, Option<T>>
    for SledReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
}

impl<IK, N, T, F, KS, TS> ReducingState<Sled, IK, N, T> for SledReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        builders::ReducingStateBuilder, serialization::NativeEndianBytesDump, sled::test::TestDb,
    };

    #[test]
    fn reducing_state_test() {
        let mut db = TestDb::new();
        let reducing_state = db.new_reducing_state(
            "test_state",
            (),
            (),
            |old: &i32, new: &i32| *old.max(new),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap().unwrap(), 42);
    }

    #[test]
    fn different_funcs_test() {
        let mut db = TestDb::new();
        let rs1 = db.new_reducing_state(
            "rs1",
            (),
            (),
            |old: &i32, new: &i32| *old.max(new),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );
        let rs2 = db.new_reducing_state(
            "rs2",
            (),
            (),
            |old: &i32, new: &i32| *old.min(new),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );

        rs1.append(&mut db, 7).unwrap();
        rs2.append(&mut db, 7).unwrap();
        rs1.append(&mut db, 42).unwrap();
        rs2.append(&mut db, 42).unwrap();
        rs1.append(&mut db, 10).unwrap();
        rs2.append(&mut db, 10).unwrap();

        assert_eq!(rs1.get(&db).unwrap().unwrap(), 42);
        assert_eq!(rs2.get(&db).unwrap().unwrap(), 7);
    }
}
