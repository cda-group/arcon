// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    backend::{Handle, InMemory, Metakey, Value, ValueOps, ValueState},
    error::*,
};
use smallbox::SmallBox;
use std::any::Any;

impl ValueOps for InMemory {
    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<()> {
        self.get_mut(handle).remove(&handle.serialize_metakeys()?);
        Ok(())
    }

    fn value_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<Option<T>> {
        if let Some(dynamic) = self.get(handle).get(&handle.serialize_metakeys()?) {
            let typed = dynamic.downcast_ref::<T>().context(InMemoryWrongType)?;
            Ok(Some(typed.clone()))
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
        let dynamic = SmallBox::new(value);
        let old_value = self.get_mut(handle).insert(key, dynamic);
        if let Some(dynamic) = old_value {
            let dynamic = dynamic as SmallBox<dyn Any, _>;
            let typed = dynamic.downcast::<T>().ok().context(InMemoryWrongType)?;
            Ok(Some(typed.into_inner()))
        } else {
            Ok(None)
        }
    }

    fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let dynamic = SmallBox::new(value);
        let _old_value = self.get_mut(handle).insert(key, dynamic);
        Ok(())
    }

    fn value_fast_set_by_ref<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: &T,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let dynamic = SmallBox::new(value.clone());
        let _old_value = self.get_mut(handle).insert(key, dynamic);
        Ok(())
    }
}
