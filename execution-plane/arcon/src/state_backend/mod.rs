// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(test)]
extern crate tempfile;
extern crate static_assertions as sa;

pub mod in_memory;
#[cfg(feature = "arcon_rocksdb")]
pub mod rocksdb;

use arcon_error::ArconResult;
use state_types::*;
use std::cell::RefCell;
use std::rc::Rc;
use serde::{Serialize, Deserialize};

// NOTE: we are using bincode for serialization, so it's probably not portable between architectures
// of different endianness

// TODO: figure out if this needs to be Send + Sync
/// Trait required for all state backend implementations in Arcon
pub trait StateBackend {
    fn create_shared(name: &str) -> ArconResult<Self>
        where Self: Sized;

    // TODO: Option instead of ArconResult? or ArconResult<Option<T>>?
    fn get(&self, key: &[u8]) -> ArconResult<Vec<u8>>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()>;
    fn remove(&mut self, key: &[u8]) -> ArconResult<()>;

    fn checkpoint(&self, id: String) -> ArconResult<()>;

    // unboxed versions of the below cannot be in the traits, because we lack GATs in Rust
    fn new_value_state_boxed<IK: 'static, N: 'static, T: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn ValueState<IK, N, T>>
        where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a>;
    fn new_map_state_boxed<IK: 'static, N: 'static, K: 'static, V: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn MapState<IK, N, K, V>>
        where IK: Serialize, N: Serialize, K: Serialize, V: Serialize, V: for<'a> Deserialize<'a>;
    fn new_vec_state_boxed<IK: 'static, N: 'static, T: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn VecState<IK, N, T>>
        where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a>;
    fn new_reducing_state_boxed<IK: 'static, N: 'static, T: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn ReducingState<IK, N, T>>
        where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a>;
    fn new_aggregating_state_boxed<IK: 'static, N: 'static, IN: 'static, OUT: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn AggregatingState<IK, N, IN, OUT>>
        where IK: Serialize, N: Serialize, IN: Serialize, OUT: for<'a> Deserialize<'a>;
}

mod state_types {
    // TODO: Q: Should methods that mutate the state actually take a mutable reference to self?
    // TODO: Q: For now this is modelled after Flink. Do we want a different hierarchy, or maybe get
    //  rid of the hierarchy altogether?

    use super::*;
    use serde::Serialize;

    //// abstract states ////
    /// State inside a stream.
    ///
    /// `IK` - type of key of the item currently in the stream
    /// `N` - type of the namespace
    /// `UK` - type of user-defined key, `()` when not `MapState`
    pub trait State<IK, N, UK = ()> {
        fn clear(&mut self) -> ArconResult<()>;

        fn get_current_key(&self) -> ArconResult<&IK>;
        fn set_current_key(&mut self, new_key: IK) -> ArconResult<()>;

        fn get_current_namespace(&self) -> ArconResult<&N>;
        fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()>;

        fn serialize_keys_and_namespace(&self, user_key: UK) -> ArconResult<Vec<u8>>
            where IK: Serialize, N: Serialize, UK: Serialize {
            bincode::serialize(&(
                self.get_current_key()?,
                self.get_current_namespace()?,
                user_key
            )).map_err(|e| arcon_err_kind!("Could not serialize keys and namespace: {}", e))
        }
    }

    pub trait AppendingState<IK, N, IN, OUT>: State<IK, N> {
        fn get(&self) -> ArconResult<OUT>;
        fn append(&mut self, value: IN) -> ArconResult<()>;
    }

    pub trait MergingState<IK, N, IN, OUT>: AppendingState<IK, N, IN, OUT> {}

    //// concrete-ish states ////

    pub trait ValueState<IK, N, T>: State<IK, N> {
        // bikeshed: get / value (Flink)
        fn get(&self) -> ArconResult<T>;

        // bikeshed: set / update (Flink)
        fn set(&mut self, new_value: T) -> ArconResult<()>;
    }

    pub trait MapState<IK, N, K, V>: State<IK, N, K> {
        fn get(&self, key: K) -> ArconResult<V>;
        fn put(&mut self, key: K, value: V) -> ArconResult<()>;

        fn put_all_dyn(&mut self, key_value_pairs: &dyn Iterator<Item=(K, V)>) -> ArconResult<()>;
        fn put_all(&mut self, key_value_pairs: impl IntoIterator<Item=(K, V)>) -> ArconResult<()>
            where Self: Sized;

        fn remove(&mut self, key: K) -> ArconResult<()>;
        fn contains(&self, key: K) -> ArconResult<bool>;

        fn iter(&self) -> ArconResult<Box<dyn Iterator<Item=(K, V)>>>;
        // makes it not object-safe :(
//        type Iter: Iterator<Item=(K, V)>;
//        fn entries_unboxed(&self) -> ArconResult<Self::Iter> where Self: Sized;

        fn keys(&self) -> ArconResult<Box<dyn Iterator<Item=K>>>;
        // makes it not object-safe :(
//        type KeysIter: Iterator<Item=K>;
//        fn keys_unboxed(&self) -> ArconResult<Self::KeysIter> where Self: Sized;

        fn values(&self) -> ArconResult<Box<dyn Iterator<Item=V>>>;
        // makes it not object-safe :(
//        type ValuesIter: Iterator<Item=V>;
//        fn values_unboxed(&self) -> ArconResult<Self::ValuesIter> where Self: Sized;

        fn is_empty(&self) -> ArconResult<bool>;
    }

    // analogous to ListState in Flink
    // TODO: Q: Should MergingState::OUT be Vec, or something else? More abstract?
    pub trait VecState<IK, N, T>: MergingState<IK, N, T, Vec<T>> {
        // bikeshed: set / update (Flink)
        fn set(&mut self, value: Vec<T>) -> ArconResult<()>;
        fn add_all(&mut self, values: impl IntoIterator<Item=T>) -> ArconResult<()>
            where Self: Sized;
        fn add_all_dyn(&mut self, values: &dyn Iterator<Item=T>) -> ArconResult<()>;
    }

    pub trait ReducingState<IK, N, T>: MergingState<IK, N, T, T> {}

    pub trait AggregatingState<IK, N, IN, OUT>: MergingState<IK, N, IN, OUT> {}

    // TODO: broadcast state???

    sa::assert_obj_safe!(
        State<u32, ()>,
        MapState<i32, (), u32, u32>,
        ValueState<i32, (), u32>,
        AppendingState<i32, (), char, String>,
        MergingState<i32, (), u32, std::collections::HashSet<u32>>,
        ReducingState<i32, (), u32>,
        VecState<i32, (), i32>,
        AggregatingState<i32, (), i32, String>
    );
}
