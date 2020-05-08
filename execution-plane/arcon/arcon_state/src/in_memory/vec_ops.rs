use crate::{
    error::*, handles::BoxedIteratorOfResult, Handle, InMemory, Metakey, Value, VecOps, VecState,
};
use smallbox::SmallBox;
use std::iter;

impl VecOps for InMemory {
    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.get_mut(handle).remove(&key);
        Ok(())
    }

    fn vec_append<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let storage = self
            .get_mut(handle)
            .entry(key)
            .or_insert_with(|| SmallBox::new(Vec::<T>::new()));
        let vec = storage
            .downcast_mut::<Vec<T>>()
            .context(InMemoryWrongType)?;

        vec.push(value);
        Ok(())
    }

    fn vec_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<Vec<T>> {
        let key = handle.serialize_metakeys()?;
        let storage = self.get(handle).get(&key);
        let vec = if let Some(storage) = storage {
            storage
                .downcast_ref::<Vec<T>>()
                .context(InMemoryWrongType)?
                .clone()
        } else {
            vec![]
        };

        Ok(vec)
    }

    fn vec_iter<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, T>> {
        let key = handle.serialize_metakeys()?;
        let storage = self.get(handle).get(&key);
        let iter: BoxedIteratorOfResult<T> = if let Some(storage) = storage {
            Box::new(
                storage
                    .downcast_ref::<Vec<T>>()
                    .context(InMemoryWrongType)?
                    .iter()
                    .cloned()
                    .map(Ok),
            )
        } else {
            Box::new(iter::empty())
        };

        Ok(iter)
    }

    fn vec_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
        value: Vec<T>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let _old_value = self.get_mut(handle).insert(key, SmallBox::new(value));
        Ok(())
    }

    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
        value: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let dynamic = self
            .get_mut(handle)
            .entry(key)
            .or_insert_with(|| SmallBox::new(Vec::<T>::new()));
        let vec = dynamic
            .downcast_mut::<Vec<T>>()
            .context(InMemoryWrongType)?;

        vec.extend(value);

        Ok(())
    }

    fn vec_len<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<usize> {
        let key = handle.serialize_metakeys()?;
        if let Some(dynamic) = self.get(handle).get(&key) {
            Ok(dynamic
                .downcast_ref::<Vec<T>>()
                .context(InMemoryWrongType)?
                .len())
        } else {
            Ok(0)
        }
    }

    fn vec_is_empty<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<bool> {
        let key = handle.serialize_metakeys()?;
        if let Some(dynamic) = self.get(handle).get(&key) {
            Ok(dynamic
                .downcast_ref::<Vec<T>>()
                .context(InMemoryWrongType)?
                .is_empty())
        } else {
            Ok(true)
        }
    }
}
