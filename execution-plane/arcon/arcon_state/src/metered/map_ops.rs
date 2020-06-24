// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, handles::BoxedIteratorOfResult, Backend, Handle, Key, MapOps, MapState, Metakey,
    Metered, Value,
};
use std::iter;

impl<B: Backend> MapOps for Metered<B> {
    measure_delegated! { MapOps:
        fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<MapState<K, V>, IK, N>,
        ) -> Result<()>;

        fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<MapState<K, V>, IK, N>,
            key: &K,
        ) -> Result<Option<V>>;

        fn map_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<MapState<K, V>, IK, N>,
            key: K,
            value: V,
        ) -> Result<()>;

        fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<MapState<K, V>, IK, N>,
            key: K,
            value: V,
        ) -> Result<Option<V>>;

        fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<MapState<K, V>, IK, N>,
            key_value_pairs: impl IntoIterator<Item = (K, V)>,
        ) -> Result<()>;

        fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<MapState<K, V>, IK, N>,
            key: &K,
        ) -> Result<Option<V>>;

        fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<MapState<K, V>, IK, N>,
            key: &K,
        ) -> Result<()>;

        fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<MapState<K, V>, IK, N>,
            key: &K,
        ) -> Result<bool>;

        fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<MapState<K, V>, IK, N>,
        ) -> Result<usize>;

        fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<MapState<K, V>, IK, N>,
        ) -> Result<bool>;
    }

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, (K, V)>> {
        let mut iter = self.measure("MapOps::map_iter", |backend| backend.map_iter(handle))?;
        let iter = iter::from_fn(move || self.measure("MapOps::map_iter::next", |_| iter.next()));
        Ok(Box::new(iter))
    }

    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, K>> {
        let mut iter = self.measure("MapOps::map_keys", |backend| backend.map_keys(handle))?;
        let iter = iter::from_fn(move || self.measure("MapOps::map_keys::next", |_| iter.next()));
        Ok(Box::new(iter))
    }

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, V>> {
        let mut iter = self.measure("MapOps::map_values", |backend| backend.map_values(handle))?;
        let iter = iter::from_fn(move || self.measure("MapOps::map_values::next", |_| iter.next()));
        Ok(Box::new(iter))
    }
}
