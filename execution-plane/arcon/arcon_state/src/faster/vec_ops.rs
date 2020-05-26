// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, handles::BoxedIteratorOfResult, serialization::protobuf, Faster, Handle, Metakey,
    Value, VecOps, VecState,
};
use std::iter;

impl VecOps for Faster {
    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        self.remove(&key)?;
        Ok(())
    }

    fn vec_append<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<VecState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        let serialized = protobuf::serialize(&value)?;

        self.vec_push(&key, serialized)
    }

    fn vec_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<Vec<T>> {
        let key = handle.serialize_id_and_metakeys()?;
        if let Some(serialized) = self.get_vec(&key)? {
            let res = serialized
                .into_iter()
                .map(|s| protobuf::deserialize(&s))
                .collect::<Result<Vec<_>>>()?;

            Ok(res)
        } else {
            Ok(vec![])
        }
    }

    fn vec_iter<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, T>> {
        let key = handle.serialize_id_and_metakeys()?;
        if let Some(serialized) = self.get_vec(&key)? {
            let res = serialized.into_iter().map(|s| protobuf::deserialize(&s));
            Ok(Box::new(res))
        } else {
            Ok(Box::new(iter::empty()))
        }
    }

    fn vec_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<VecState<T>, IK, N>,
        value: Vec<T>,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        let storage = value
            .into_iter()
            .map(|v| protobuf::serialize(&v))
            .collect::<Result<Vec<_>>>()?;

        self.vec_set(&key, storage)
    }

    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<VecState<T>, IK, N>,
        values: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        let storage = values
            .into_iter()
            .map(|v| protobuf::serialize(&v))
            .collect::<Result<Vec<_>>>()?;

        self.vec_push_all(&key, storage)?;

        Ok(())
    }

    fn vec_len<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<usize> {
        let key = handle.serialize_id_and_metakeys()?;
        Ok(self.get_vec(&key)?.map(|v| v.len()).unwrap_or(0))
    }

    fn vec_is_empty<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<bool> {
        let key = handle.serialize_id_and_metakeys()?;
        Ok(self.get_vec(&key)?.map(|v| v.is_empty()).unwrap_or(true))
    }
}
