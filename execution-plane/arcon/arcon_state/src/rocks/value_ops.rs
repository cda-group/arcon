// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, serialization::protobuf, Handle, Metakey, Rocks, Value, ValueOps, ValueState,
};

impl ValueOps for Rocks {
    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.remove(handle.id, &key)?;
        Ok(())
    }

    fn value_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<Option<T>> {
        let key = handle.serialize_metakeys()?;
        if let Some(serialized) = self.get(handle.id, &key)? {
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn value_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<Option<T>> {
        let key = handle.serialize_metakeys()?;
        let old = if let Some(serialized) = self.get(handle.id, &key)? {
            let value = protobuf::deserialize(&serialized)?;
            Some(value)
        } else {
            None
        };
        let serialized = protobuf::serialize(&value)?;
        self.put(handle.id, key, serialized)?;
        Ok(old)
    }

    fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let serialized = protobuf::serialize(&value)?;
        self.put(handle.id, key, serialized)?;
        Ok(())
    }
}
