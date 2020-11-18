// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{Metakey, Value},
    error::*,
    serialization::protobuf,
    sled::Sled,
    Handle, ValueOps, ValueState,
};

impl ValueOps for Sled {
    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &self,
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
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<Option<T>> {
        let key = handle.serialize_metakeys()?;
        let serialized = protobuf::serialize(&value)?;
        let old = match self.put(handle.id, &key, &serialized)? {
            Some(bytes) => Some(protobuf::deserialize(bytes.as_ref())?),
            None => None,
        };
        Ok(old)
    }

    fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let serialized = protobuf::serialize(&value)?;
        self.put(handle.id, &key, &serialized)?;
        Ok(())
    }

    fn value_fast_set_by_ref<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: &T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let serialized = protobuf::serialize(value)?;
        self.put(handle.id, &key, &serialized)?;
        Ok(())
    }
}
