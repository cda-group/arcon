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

bundle! {
    #[allow(missing_debug_implementations)]
    pub struct InMemoryTestStruct<F: Reducer<u32>> {
        value: Handle<ValueState<u32>, u32, u32>,
        value2: Handle<ValueState<u32>, u32, u32>,
        map: Handle<MapState<String, i32>, u32, u32>,
        vec: Handle<VecState<u32>, u32, u32>,
        reducer: Handle<ReducerState<u32, F>, u32, u32>,
        aggregator: Handle<AggregatorState<TestAggregator>, u32, u32>
    }
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
            use crate::{test_common::*, Bundle, RegistrationToken};
            use std::collections::HashSet;

            #[test]
            fn value_state_test() {
                let mut db = $construct_backend;
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
            fn value_states_are_independant() {
                let mut db = $construct_backend;
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
            fn value_states_handle_state_for_different_keys_and_namespaces() {
                let mut db = $construct_backend;
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
                let mut db = $construct_backend;
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
            fn map_clearing_test() {
                let mut db = $construct_backend;
                let mut session = db.session();
                let mut bundle = bundle();
                bundle.register_states(&mut session, unsafe { &RegistrationToken::new() });
                let mut bundle = bundle.activate(&mut session);

                let mut expected_for_key_zero = HashSet::new();
                for i in 0..10 {
                    let key = i.to_string();
                    let value = i;
                    expected_for_key_zero.insert((key.clone(), value));
                    bundle.map().fast_insert(key, value).unwrap()
                }

                bundle.map().set_item_key(1);

                let mut expected_for_key_one = HashSet::new();
                for i in 10..20 {
                    let key = i.to_string();
                    let value = i;
                    expected_for_key_one.insert((key.clone(), value));
                    bundle.map().fast_insert(key, value).unwrap()
                }

                let tuples_from_key_one: HashSet<_> =
                    bundle.map().iter().unwrap().map(Result::unwrap).collect();
                assert_eq!(tuples_from_key_one, expected_for_key_one);

                bundle.map().set_item_key(0);
                let tuples_from_key_zero: HashSet<_> =
                    bundle.map().iter().unwrap().map(Result::unwrap).collect();
                assert_eq!(tuples_from_key_zero, expected_for_key_zero);

                bundle.map().clear().unwrap();
                assert!(bundle.map().is_empty().unwrap());

                bundle.map().set_item_key(1);
                let tuples_from_key_one_after_clear_zero: HashSet<_> =
                    bundle.map().iter().unwrap().map(Result::unwrap).collect();
                assert_eq!(tuples_from_key_one_after_clear_zero, expected_for_key_one);
            }

            #[test]
            fn vec_state_test() {
                let mut db = $construct_backend;
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

                bundle.vec().set(vec![42, 69]).unwrap();
                assert_eq!(bundle.vec().get().unwrap(), vec![42, 69]);
            }

            #[test]
            fn reducing_state_test() {
                let mut db = $construct_backend;
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
                let mut db = $construct_backend;
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
    };
}
