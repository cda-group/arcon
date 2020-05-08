// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    error::*, Aggregator, AggregatorState, Backend, Config, Handle, Key, MapState, Metakey,
    Reducer, ReducerState, StateType, Value, ValueState, VecState,
};
use smallbox::{space, SmallBox};
use std::{any::Any, collections::HashMap, path::Path};

// we'll store values of size up to 8 * size_of::<usize>() inline
type StoredValue = SmallBox<dyn Any + Send, space::S8>;

#[derive(Debug, Default)]
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
        Ok(Self::default())
    }

    fn create(_path: &Path) -> Result<Self, ArconStateError>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }

    fn restore(_live_path: &Path, _checkpoint_path: &Path) -> Result<Self, ArconStateError>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }

    fn checkpoint(&self, _checkpoint_path: &Path) -> Result<(), ArconStateError> {
        Ok(())
    }

    // region handle activators
    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }

    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }
    // endregion
}

mod aggregator_ops;
mod map_ops;
mod reducer_ops;
mod value_ops;
mod vec_ops;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bundle, Bundle, RegistrationToken};

    #[derive(Clone)]
    struct TestAggregator;
    impl Aggregator for TestAggregator {
        type Input = u32;
        type Accumulator = Vec<u8>;
        type Result = String;

        fn create_accumulator(&self) -> Self::Accumulator {
            vec![]
        }

        fn add(&self, acc: &mut Self::Accumulator, value: Self::Input) {
            acc.push(value as u8)
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

    fn bundle() -> InMemoryTestStruct<impl Fn(&u32, &u32) -> u32 + Clone> {
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
