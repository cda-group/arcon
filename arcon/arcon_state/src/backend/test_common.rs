use crate::{
    Aggregator, AggregatorState, Handle, MapState, Reducer, ReducerState, ValueState, VecState,
};

#[derive(Debug, Clone)]
pub struct TestAggregator;
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

pub struct InMemoryTestStruct<F: Reducer<u32>> {
    pub value: Handle<ValueState<u32>, u32, u32>,
    pub value2: Handle<ValueState<u32>, u32, u32>,
    pub map: Handle<MapState<String, i32>, u32, u32>,
    pub vec: Handle<VecState<u32>, u32, u32>,
    pub reducer: Handle<ReducerState<u32, F>, u32, u32>,
    pub aggregator: Handle<AggregatorState<TestAggregator>, u32, u32>,
}

pub fn bundle() -> InMemoryTestStruct<impl Reducer<u32>> {
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

#[macro_export]
macro_rules! common_state_tests {
    ($construct_backend: expr) => {
        mod common {
            use super::*;
            use crate::backend::test_common::*;
            use std::collections::HashSet;

            #[test]
            fn value_state_test() {
                let db = $construct_backend;
                let mut bundle = bundle();

                db.register_value_handle(&mut bundle.value);
                let mut value = bundle.value.activate(db.clone());

                let unset = value.get().unwrap();
                assert!(unset.is_none());

                value.set(123).unwrap();
                let set = value.get().unwrap().unwrap();
                assert_eq!(set, 123);

                value.clear().unwrap();
                let cleared = value.get().unwrap();
                assert!(cleared.is_none());
            }

            #[test]
            fn value_states_are_independant() {
                let db = $construct_backend;
                let mut bundle = bundle();

                db.register_value_handle(&mut bundle.value);
                db.register_value_handle(&mut bundle.value2);

                let mut value = bundle.value.activate(db.clone());
                let mut value2 = bundle.value2.activate(db.clone());

                value.set(123).unwrap();
                value2.set(456).unwrap();

                let v1v = value.get().unwrap().unwrap();
                let v2v = value2.get().unwrap().unwrap();
                assert_eq!(v1v, 123);
                assert_eq!(v2v, 456);

                value.clear().unwrap();
                let v1opt = value.get().unwrap();
                let v2v = value2.get().unwrap().unwrap();
                assert!(v1opt.is_none());
                assert_eq!(v2v, 456);
            }

            #[test]
            fn value_states_handle_state_for_different_keys_and_namespaces() {
                let db = $construct_backend;
                let mut bundle = bundle();
                db.register_value_handle(&mut bundle.value);
                let mut value = bundle.value.activate(db.clone());

                value.set(0).unwrap();
                value.set_item_key(1);
                let should_be_none = value.get().unwrap();
                assert!(should_be_none.is_none());

                value.set(1).unwrap();
                let should_be_one = value.get().unwrap().unwrap();
                assert_eq!(should_be_one, 1);

                value.set_item_key(0);
                let should_be_zero = value.get().unwrap().unwrap();
                assert_eq!(should_be_zero, 0);

                value.set_namespace(1);
                let should_be_none = value.get().unwrap();
                assert!(should_be_none.is_none());

                value.set(2).unwrap();
                let should_be_two = value.get().unwrap().unwrap();
                assert_eq!(should_be_two, 2);

                value.set_namespace(0);
                let should_be_zero = value.get().unwrap().unwrap();
                assert_eq!(should_be_zero, 0);
            }

            #[test]
            fn map_state_test() {
                let db = $construct_backend;
                let mut bundle = bundle();
                db.register_map_handle(&mut bundle.map);
                let map = bundle.map.activate(db.clone());

                // TODO: &String is weird, maybe look at how it's done with the keys in std hash-map
                assert!(!map.contains(&"first key".to_string()).unwrap());

                map.fast_insert("first key".to_string(), 42).unwrap();
                map.fast_insert("second key".to_string(), 69).unwrap();

                assert!(map.contains(&"first key".to_string()).unwrap());
                assert!(map.contains(&"second key".to_string()).unwrap());

                assert_eq!(map.get(&"first key".to_string()).unwrap().unwrap(), 42);
                assert_eq!(map.get(&"second key".to_string()).unwrap().unwrap(), 69);

                assert_eq!(map.len().unwrap(), 2);

                let keys: Vec<_> = map.keys().unwrap().map(Result::unwrap).collect();

                assert_eq!(keys.len(), 2);
                assert!(keys.contains(&"first key".to_string()));
                assert!(keys.contains(&"second key".to_string()));

                map.clear().unwrap();
                assert_eq!(map.len().unwrap(), 0);
                assert!(map.is_empty().unwrap());
            }

            #[test]
            fn map_clearing_test() {
                let db = $construct_backend;
                let mut bundle = bundle();
                db.register_map_handle(&mut bundle.map);
                let mut map = bundle.map.activate(db.clone());

                let mut expected_for_key_zero = HashSet::new();
                for i in 0..10 {
                    let key = i.to_string();
                    let value = i;
                    expected_for_key_zero.insert((key.clone(), value));
                    map.fast_insert(key, value).unwrap()
                }

                map.set_item_key(1);

                let mut expected_for_key_one = HashSet::new();
                for i in 10..20 {
                    let key = i.to_string();
                    let value = i;
                    expected_for_key_one.insert((key.clone(), value));
                    map.fast_insert(key, value).unwrap()
                }

                let tuples_from_key_one: HashSet<_> =
                    map.iter().unwrap().map(Result::unwrap).collect();
                assert_eq!(tuples_from_key_one, expected_for_key_one);

                map.set_item_key(0);
                let tuples_from_key_zero: HashSet<_> =
                    map.iter().unwrap().map(Result::unwrap).collect();
                assert_eq!(tuples_from_key_zero, expected_for_key_zero);

                map.clear().unwrap();
                assert!(map.is_empty().unwrap());

                map.set_item_key(1);
                let tuples_from_key_one_after_clear_zero: HashSet<_> =
                    map.iter().unwrap().map(Result::unwrap).collect();
                assert_eq!(tuples_from_key_one_after_clear_zero, expected_for_key_one);
            }

            #[test]
            fn vec_state_test() {
                let db = $construct_backend;
                let mut bundle = bundle();
                db.register_vec_handle(&mut bundle.vec);
                let vec = bundle.vec.activate(db.clone());

                assert!(vec.is_empty().unwrap());
                assert_eq!(vec.len().unwrap(), 0);

                vec.append(1).unwrap();
                vec.append(2).unwrap();
                vec.append(3).unwrap();
                vec.add_all(vec![4, 5, 6]).unwrap();

                assert_eq!(vec.get().unwrap(), vec![1, 2, 3, 4, 5, 6]);
                assert_eq!(vec.len().unwrap(), 6);

                vec.set(vec![42, 69]).unwrap();
                assert_eq!(vec.get().unwrap(), vec![42, 69]);
                assert_eq!(
                    vec.iter().unwrap().collect::<Result<Vec<_>>>().unwrap(),
                    vec![42, 69]
                );
            }

            #[test]
            fn reducing_state_test() {
                let db = $construct_backend;
                let mut bundle = bundle();
                db.register_reducer_handle(&mut bundle.reducer);
                let reducer = bundle.reducer.activate(db.clone());

                reducer.reduce(7).unwrap();
                reducer.reduce(42).unwrap();
                reducer.reduce(10).unwrap();

                assert_eq!(reducer.get().unwrap().unwrap(), 42);
            }

            #[test]
            fn aggregating_state_test() {
                let db = $construct_backend;
                let mut bundle = bundle();
                db.register_aggregator_handle(&mut bundle.aggregator);
                let aggregator = bundle.aggregator.activate(db.clone());

                aggregator.aggregate(1).unwrap();
                aggregator.aggregate(2).unwrap();
                aggregator.aggregate(3).unwrap();

                assert_eq!(aggregator.get().unwrap(), "[1, 2, 3]".to_string());
            }
        }
    };
}
