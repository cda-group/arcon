use crate::{
    error::*, serialization::protobuf, Faster, Handle, Metakey, Value, ValueOps, ValueState,
};

impl ValueOps for Faster {
    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        self.remove(&key)?;
        Ok(())
    }

    fn value_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<Option<T>> {
        let key = handle.serialize_id_and_metakeys()?;
        if let Some(serialized) = self.get(&key)? {
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
        let key = handle.serialize_id_and_metakeys()?;
        let old = if let Some(old_serialized) = self.get(&key)? {
            let old_value = protobuf::deserialize(&old_serialized)?;
            Some(old_value)
        } else {
            None
        };
        let serialized = protobuf::serialize(&value)?;
        self.put(&key, &serialized)?;
        Ok(old)
    }

    fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        let serialized = protobuf::serialize(&value)?;
        self.put(&key, &serialized)?;
        Ok(())
    }
}
