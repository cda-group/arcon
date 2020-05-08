use crate::{error::*, Handle, InMemory, Metakey, Reducer, ReducerOps, ReducerState, Value};
use smallbox::SmallBox;

impl ReducerOps for InMemory {
    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let _old_value = self.get_mut(handle).remove(&key);
        Ok(())
    }

    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>> {
        let key = handle.serialize_metakeys()?;
        if let Some(dynamic) = self.get(handle).get(&key) {
            let value = dynamic
                .downcast_ref::<T>()
                .context(InMemoryWrongType)?
                .clone();

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn reducer_reduce<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ReducerState<T, F>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        match self.get_mut(handle).get_mut(&key) {
            None => {
                let _old_value_which_obviously_is_none =
                    self.get_mut(handle).insert(key, SmallBox::new(value));
            }
            Some(dynamic) => {
                let old = dynamic.downcast_mut::<T>().context(InMemoryWrongType)?;
                *old = (handle.extra_data)(old, &value);
            }
        }

        Ok(())
    }
}
