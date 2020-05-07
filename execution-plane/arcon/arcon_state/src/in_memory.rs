// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    error::*,
    handles::{ActiveHandle, BoxedIteratorOfResult},
    serialization::*,
    Aggregator, AggregatorState, Backend, Config, Handle, Key, MapState, Metakey, Reducer,
    ReducerState, RegistrationToken, StateType, Value, ValueState, VecState,
};
use smallbox::{space, SmallBox};
use std::{any::Any, collections::HashMap, iter};

// we'll store values of size up to 8 * size_of::<usize>() inline
type StoredValue = SmallBox<dyn Any + Send, space::S8>;

#[derive(Debug)]
pub struct InMemory {
    data: HashMap<&'static str, HashMap<Vec<u8>, StoredValue>>,
}

impl InMemory {
    fn get<S: StateType, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<S, IK, N>,
    ) -> &HashMap<Vec<u8>, StoredValue> {
        self.data.get(handle.id).unwrap()
    }
    fn get_mut<S: StateType, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<S, IK, N>,
    ) -> &mut HashMap<Vec<u8>, StoredValue> {
        self.data.get_mut(handle.id).unwrap()
    }
}

impl Backend for InMemory {
    fn restore_or_create(_config: &Config, _id: String) -> Result<Self> {
        Ok(InMemory {
            data: Default::default(),
        })
    }

    fn checkpoint(&self, _config: &Config) -> Result<(), ArconStateError> {
        Ok(())
    }

    // region handle activators
    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        self.data.entry(inner.id).or_default();
        inner.registered = true;
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        self.data.entry(inner.id).or_default();
        inner.registered = true;
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        self.data.entry(inner.id).or_default();
        inner.registered = true;
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        self.data.entry(inner.id).or_default();
        inner.registered = true;
    }

    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) {
        self.data.entry(inner.id).or_default();
        inner.registered = true;
    }
    // endregion

    // region value ops
    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ValueState<T>, IK, N>,
    ) -> Result<(), ArconStateError> {
        self.get_mut(handle).remove(&handle.serialize_metakeys()?);
        Ok(())
    }

    fn value_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<Option<T>, ArconStateError> {
        if let Some(dynamic) = self.get(handle).get(&handle.serialize_metakeys()?) {
            let typed = dynamic.downcast_ref::<T>().context(InMemoryWrongType)?;
            Ok(Some(typed.clone()))
        } else {
            Ok(None)
        }
    }

    fn value_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<Option<T>, ArconStateError> {
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
        handle: &mut Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys()?;
        let dynamic = SmallBox::new(value);
        let _old_value = self.get_mut(handle).insert(key, dynamic);
        Ok(())
    }

    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys()?;
        self.get_mut(handle)
            .retain(|k, _| &k[..key.len()] != &key[..]);
        Ok(())
    }

    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>, ArconStateError> {
        let key = handle.serialize_metakeys_and_key(key)?;
        if let Some(dynamic) = self.get(handle).get(&key) {
            let value = dynamic
                .downcast_ref::<V>()
                .context(InMemoryWrongType)?
                .clone();

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn map_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys_and_key(&key)?;
        let dynamic = SmallBox::new(value);
        let _old_value = self.get_mut(handle).insert(key, dynamic);

        Ok(())
    }

    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<Option<V>, ArconStateError> {
        let key = handle.serialize_metakeys_and_key(&key)?;

        let new_dynamic = SmallBox::new(value);
        let old = match self.get_mut(handle).insert(key, new_dynamic) {
            None => None,
            Some(old_dynamic) => Some(
                old_dynamic
                    .downcast_ref::<V>()
                    .context(InMemoryWrongType)?
                    .clone(),
            ),
        };

        Ok(old)
    }

    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), ArconStateError> {
        for (k, v) in key_value_pairs.into_iter() {
            self.map_fast_insert(handle, k, v)?; // TODO: what if one fails? partial insert? should we roll back?
        }

        Ok(())
    }

    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>, ArconStateError> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let old_value = self.get_mut(handle).remove(&key);
        if let Some(dynamic) = old_value {
            let dynamic = dynamic as SmallBox<dyn Any, _>;
            let typed = dynamic.downcast::<V>().ok().context(InMemoryWrongType)?;
            Ok(Some(typed.into_inner()))
        } else {
            Ok(None)
        }
    }

    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let _old_value = self.get_mut(handle).remove(&key);

        Ok(())
    }

    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<bool, ArconStateError> {
        let key = handle.serialize_metakeys_and_key(key)?;
        Ok(self.get(handle).contains_key(&key))
    }

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, (K, V)>, ArconStateError> {
        let prefix = handle.serialize_metakeys()?;
        let iter = self
            .get(handle)
            .iter()
            .filter_map(move |(k, v)| {
                if &k[..prefix.len()] != &prefix[..] {
                    return None;
                }

                Some((k.as_slice(), &**v))
            })
            .map(move |(k, v)| {
                let mut cursor = k;
                let _item_key: IK = fixed_bytes::deserialize_from(&mut cursor)?;
                let _namespace: N = fixed_bytes::deserialize_from(&mut cursor)?;
                let key: K = protobuf::deserialize_from(&mut cursor)?;
                let value = v.downcast_ref::<V>().context(InMemoryWrongType)?.clone();
                Ok((key, value))
            });

        Ok(Box::new(iter))
    }

    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, K>, ArconStateError> {
        let prefix = handle.serialize_metakeys()?;
        let iter = self
            .get(handle)
            .iter()
            .filter_map(move |(k, _v)| {
                if &k[..prefix.len()] != &prefix[..] {
                    return None;
                }

                Some(k.as_slice())
            })
            .map(move |k| {
                let mut cursor = k;
                let _item_key: IK = fixed_bytes::deserialize_from(&mut cursor)?;
                let _namespace: N = fixed_bytes::deserialize_from(&mut cursor)?;
                let key: K = protobuf::deserialize_from(&mut cursor)?;
                Ok(key)
            });

        Ok(Box::new(iter))
    }

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, V>, ArconStateError> {
        let prefix = handle.serialize_metakeys()?;
        let iter = self
            .get(handle)
            .iter()
            .filter_map(move |(k, v)| {
                if &k[..prefix.len()] != &prefix[..] {
                    return None;
                }

                Some(&**v)
            })
            .map(move |v| {
                let value = v.downcast_ref::<V>().context(InMemoryWrongType)?.clone();
                Ok(value)
            });

        Ok(Box::new(iter))
    }

    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize, ArconStateError> {
        let prefix = handle.serialize_metakeys()?;
        let count = self
            .get(handle)
            .iter()
            .filter(move |(k, _v)| &k[..prefix.len()] == &prefix[..])
            .count();
        Ok(count)
    }

    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool, ArconStateError> {
        let prefix = handle.serialize_metakeys()?;
        let is_empty = self
            .get(handle)
            .iter()
            .filter(move |(k, _v)| &k[..prefix.len()] == &prefix[..])
            .next()
            .is_none();
        Ok(is_empty)
    }

    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys()?;
        self.get_mut(handle).remove(&key);
        Ok(())
    }

    fn vec_append<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
        value: T,
    ) -> Result<(), ArconStateError> {
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
    ) -> Result<Vec<T>, ArconStateError> {
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
    ) -> Result<BoxedIteratorOfResult<'_, T>, ArconStateError> {
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
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys()?;
        let _old_value = self.get_mut(handle).insert(key, SmallBox::new(value));
        Ok(())
    }

    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
        value: impl IntoIterator<Item = T>,
    ) -> Result<(), ArconStateError> {
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
    ) -> Result<usize, ArconStateError> {
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
    ) -> Result<bool, ArconStateError> {
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

    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys()?;
        let _old_value = self.get_mut(handle).remove(&key);
        Ok(())
    }

    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>, ArconStateError> {
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
    ) -> Result<(), ArconStateError> {
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

    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys()?;
        let _old_value = self.get_mut(handle).remove(&key);
        Ok(())
    }

    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<<A as Aggregator>::Result, ArconStateError> {
        let key = handle.serialize_metakeys()?;
        if let Some(dynamic) = self.get(handle).get(&key) {
            let current_accumulator = dynamic
                .downcast_ref::<A::Accumulator>()
                .context(InMemoryWrongType)?
                .clone();
            Ok(handle
                .extra_data
                .accumulator_into_result(current_accumulator))
        } else {
            Ok(handle
                .extra_data
                .accumulator_into_result(handle.extra_data.create_accumulator()))
        }
    }

    fn aggregator_aggregate<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
        value: <A as Aggregator>::Input,
    ) -> Result<(), ArconStateError> {
        let key = handle.serialize_metakeys()?;
        let current_accumulator = self
            .get_mut(handle)
            .entry(key)
            .or_insert_with(|| SmallBox::new(handle.extra_data.create_accumulator()))
            .downcast_mut::<A::Accumulator>()
            .context(InMemoryWrongType)?;

        handle.extra_data.add(current_accumulator, value);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bundle, Bundle};

    struct TestAggregator;
    impl Aggregator for TestAggregator {
        type Input = u8;
        type Accumulator = Vec<u8>;
        type Result = String;

        fn create_accumulator(&self) -> Self::Accumulator {
            vec![]
        }

        fn add(&self, acc: &mut Self::Accumulator, value: Self::Input) {
            acc.push(value)
        }

        fn merge_accumulators(
            &self,
            mut fst: Self::Accumulator,
            mut snd: Self::Accumulator,
        ) -> Self::Accumulator {
            fst.append(&mut snd);
            fst
        }

        fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result {
            format!("{:?}", acc)
        }
    }

    bundle! {
        struct InMemoryTestStruct<F: Reducer<u32>> {
            value: Handle<ValueState<u32>, u32, u32>,
            value2: Handle<ValueState<u32>, u32, u32>,
            map: Handle<MapState<String, i32>, u32, u32>,
            vec: Handle<VecState<u32>, u32, u32>,
            reducer: Handle<ReducerState<u32, F>, u32, u32>,
            aggregator: Handle<AggregatorState<TestAggregator>, u32, u32>
        }
    }

    fn bundle() -> InMemoryTestStruct<impl Fn(&u32, &u32) -> u32> {
        InMemoryTestStruct {
            value: Handle::value("value").with_item_key(0).with_namespace(0),
            value2: Handle::value("value2").with_item_key(0).with_namespace(0),
            map: Handle::map("map").with_item_key(0).with_namespace(0),
            vec: Handle::vec("vec").with_item_key(0).with_namespace(0),
            reducer: Handle::reducer("reducer", |a: &u32, b: &u32| *a.max(b))
                .with_item_key(0)
                .with_namespace(0),
            aggregator: Handle::aggregator("aggregator", TestAggregator)
                .with_item_key(0)
                .with_namespace(0),
        }
    }

    #[test]
    fn in_memory_value_state_test() {
        let mut db = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
        let mut session = db.session();
        let mut bundle = bundle();
        bundle.register_states(&mut session, unsafe { &RegistrationToken::new() });
        let mut bundle = bundle.activate(&mut session);

        let unset = bundle.value().get().unwrap();
        assert!(unset.is_none());

        bundle.value().set(123).unwrap();
        let set = bundle.value().get().unwrap().unwrap();
        assert_eq!(set, 123);

        bundle.value().clear().unwrap();
        let cleared = bundle.value().get().unwrap();
        assert!(cleared.is_none());
    }

    #[test]
    fn in_memory_value_states_are_independant() {
        let mut db = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
        let mut session = db.session();
        let mut bundle = bundle();
        bundle.register_states(&mut session, unsafe { &RegistrationToken::new() });
        let mut bundle = bundle.activate(&mut session);

        bundle.value().set(123).unwrap();
        bundle.value2().set(456).unwrap();

        let v1v = bundle.value().get().unwrap().unwrap();
        let v2v = bundle.value2().get().unwrap().unwrap();
        assert_eq!(v1v, 123);
        assert_eq!(v2v, 456);

        bundle.value().clear().unwrap();
        let v1opt = bundle.value().get().unwrap();
        let v2v = bundle.value2().get().unwrap().unwrap();
        assert!(v1opt.is_none());
        assert_eq!(v2v, 456);
    }

    #[test]
    fn in_memory_value_states_handle_state_for_different_keys_and_namespaces() {
        let mut db = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
        let mut session = db.session();
        let mut bundle = bundle();
        bundle.register_states(&mut session, unsafe { &RegistrationToken::new() });
        let mut bundle = bundle.activate(&mut session);

        bundle.value().set(0).unwrap();
        bundle.value().set_item_key(1);
        let should_be_none = bundle.value().get().unwrap();
        assert!(should_be_none.is_none());

        bundle.value().set(1).unwrap();
        let should_be_one = bundle.value().get().unwrap().unwrap();
        assert_eq!(should_be_one, 1);

        bundle.value().set_item_key(0);
        let should_be_zero = bundle.value().get().unwrap().unwrap();
        assert_eq!(should_be_zero, 0);

        bundle.value().set_namespace(1);
        let should_be_none = bundle.value().get().unwrap();
        assert!(should_be_none.is_none());

        bundle.value().set(2).unwrap();
        let should_be_two = bundle.value().get().unwrap().unwrap();
        assert_eq!(should_be_two, 2);

        bundle.value().set_namespace(0);
        let should_be_zero = bundle.value().get().unwrap().unwrap();
        assert_eq!(should_be_zero, 0);
    }

    #[test]
    fn map_state_test() {
        let mut db = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
        let mut session = db.session();
        let mut bundle = bundle();
        bundle.register_states(&mut session, unsafe { &RegistrationToken::new() });
        let mut bundle = bundle.activate(&mut session);

        // TODO: &String is weird, maybe look at how it's done with the keys in std hash-map
        assert!(!bundle.map().contains(&"first key".to_string()).unwrap());

        bundle
            .map()
            .fast_insert("first key".to_string(), 42)
            .unwrap();
        bundle
            .map()
            .fast_insert("second key".to_string(), 69)
            .unwrap();

        assert!(bundle.map().contains(&"first key".to_string()).unwrap());
        assert!(bundle.map().contains(&"second key".to_string()).unwrap());

        assert_eq!(
            bundle.map().get(&"first key".to_string()).unwrap().unwrap(),
            42
        );
        assert_eq!(
            bundle
                .map()
                .get(&"second key".to_string())
                .unwrap()
                .unwrap(),
            69
        );

        assert_eq!(bundle.map().len().unwrap(), 2);

        let keys: Vec<_> = bundle.map().keys().unwrap().map(Result::unwrap).collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"first key".to_string()));
        assert!(keys.contains(&"second key".to_string()));

        bundle.map().clear().unwrap();
        assert_eq!(bundle.map().len().unwrap(), 0);
        assert!(bundle.map().is_empty().unwrap());
    }

    #[test]
    fn vec_state_test() {
        let mut db = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
        let mut session = db.session();
        let mut bundle = bundle();
        bundle.register_states(&mut session, unsafe { &RegistrationToken::new() });
        let mut bundle = bundle.activate(&mut session);

        assert!(bundle.vec().is_empty().unwrap());
        assert_eq!(bundle.vec().len().unwrap(), 0);

        bundle.vec().append(1).unwrap();
        bundle.vec().append(2).unwrap();
        bundle.vec().append(3).unwrap();
        bundle.vec().add_all(vec![4, 5, 6]).unwrap();

        assert_eq!(bundle.vec().get().unwrap(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(bundle.vec().len().unwrap(), 6);
    }

    #[test]
    fn reducing_state_test() {
        let mut db = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
        let mut session = db.session();
        let mut bundle = bundle();
        bundle.register_states(&mut session, unsafe { &RegistrationToken::new() });
        let mut bundle = bundle.activate(&mut session);

        bundle.reducer().reduce(7).unwrap();
        bundle.reducer().reduce(42).unwrap();
        bundle.reducer().reduce(10).unwrap();

        assert_eq!(bundle.reducer().get().unwrap().unwrap(), 42);
    }

    #[test]
    fn aggregating_state_test() {
        let mut db = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
        let mut session = db.session();
        let mut bundle = bundle();
        bundle.register_states(&mut session, unsafe { &RegistrationToken::new() });
        let mut bundle = bundle.activate(&mut session);

        bundle.aggregator().aggregate(1).unwrap();
        bundle.aggregator().aggregate(2).unwrap();
        bundle.aggregator().aggregate(3).unwrap();

        assert_eq!(bundle.aggregator().get().unwrap(), "[1, 2, 3]".to_string());
    }
}
