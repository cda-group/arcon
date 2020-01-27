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

// TODO: a lot of params here could be borrows
// TODO: figure out if this needs to be Send + Sync
/// Trait required for all state backend implementations in Arcon
pub trait StateBackend {
    fn new(name: &str) -> ArconResult<Self>
        where Self: Sized;

    // TODO: Option instead of ArconResult? or ArconResult<Option<T>>?
    fn get_cloned(&self, key: &[u8]) -> ArconResult<Vec<u8>>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()>;
    fn remove(&mut self, key: &[u8]) -> ArconResult<()>;

    fn checkpoint(&self, id: String) -> ArconResult<()>;

    // unboxed versions of the below cannot be in the traits, because we lack GATs in Rust
//    fn new_value_state_boxed<IK: 'static, N: 'static, T: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn ValueState<IK, N, T>>
//        where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a>;
//    fn new_map_state_boxed<IK: 'static, N: 'static, K: 'static, V: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn MapState<IK, N, K, V>>
//        where IK: Serialize, N: Serialize, K: Serialize, V: Serialize, V: for<'a> Deserialize<'a>;
//    fn new_vec_state_boxed<IK: 'static, N: 'static, T: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn VecState<IK, N, T>>
//        where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a>;
//    fn new_reducing_state_boxed<IK: 'static, N: 'static, T: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn ReducingState<IK, N, T>>
//        where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a>;
//    fn new_aggregating_state_boxed<IK: 'static, N: 'static, IN: 'static, OUT: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn AggregatingState<IK, N, IN, OUT>>
//        where IK: Serialize, N: Serialize, IN: Serialize, OUT: for<'a> Deserialize<'a>;
}

// this is hackish as hell, because we don't have GATs
pub trait ConcreteStateBackend<IK, N, T1, T2>: Sized {
    // T1 and T2 mean different things for different types, T2 is sometimes unused.

    type ConcreteValueState: ValueState<Self, IK, N, T1>;
    fn new_value_state(&self, init_item_key: IK, init_namespace: N) -> Self::ConcreteValueState;

    type ConcreteMapState: MapState<Self, IK, N, T1, T2>;
    fn new_map_state(&self, init_item_key: IK, init_namespace: N) -> Self::ConcreteMapState;

    type ConcreteVecState: VecState<Self, IK, N, T1>;
    fn new_vec_state(&self, init_item_key: IK, init_namespace: N) -> Self::ConcreteVecState;

    type ConcreteReducingState: ReducingState<Self, IK, N, T1>;
    fn new_reducing_state(&self, init_item_key: IK, init_namespace: N) -> Self::ConcreteReducingState;

    type ConcreteAggregatingState: AggregatingState<Self, IK, N, T1, T2>;
    fn new_aggregating_state(&self, init_item_key: IK, init_namespace: N) -> Self::ConcreteAggregatingState;
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
    /// `SB` - state backend type
    pub trait State<SB, IK, N> {
        fn clear(&self, backend: &mut SB) -> ArconResult<()>;

        fn get_current_key(&self) -> ArconResult<&IK>;
        fn set_current_key(&mut self, new_key: IK) -> ArconResult<()>;

        fn get_current_namespace(&self) -> ArconResult<&N>;
        fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()>;
    }

    // TODO: since we don't have any state that is appending, but not merging, maybe consider using one trait?
    pub trait AppendingState<SB, IK, N, IN, OUT>: State<SB, IK, N> {
        fn get(&self, backend: &SB) -> ArconResult<OUT>;
        fn append(&self, backend: &mut SB, value: IN) -> ArconResult<()>;
    }

    pub trait MergingState<SB, IK, N, IN, OUT>: AppendingState<SB, IK, N, IN, OUT> {}

    //// concrete-ish states ////

    pub trait ValueState<SB, IK, N, T>: State<SB, IK, N> {
        // bikeshed: get / value (Flink)
        fn get(&self, backend: &SB) -> ArconResult<T>;

        // bikeshed: set / update (Flink)
        fn set(&self, backend: &mut SB, new_value: T) -> ArconResult<()>;
    }

    pub trait MapState<SB, IK, N, K, V>: State<SB, IK, N> {
        fn get(&self, backend: &SB, key: &K) -> ArconResult<V>;
        fn put(&self, backend: &mut SB, key: K, value: V) -> ArconResult<()>;

        fn put_all_dyn(&self, backend: &mut SB, key_value_pairs: &mut dyn Iterator<Item=(K, V)>) -> ArconResult<()>;
        fn put_all(&self, backend: &mut SB, key_value_pairs: impl IntoIterator<Item=(K, V)>) -> ArconResult<()>
            where Self: Sized;

        fn remove(&self, backend: &mut SB, key: &K) -> ArconResult<()>;
        fn contains(&self, backend: &SB, key: &K) -> ArconResult<bool>;

        fn iter<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item=(K, V)> + 'a>>;
        // makes it not object-safe :(
//        type Iter: Iterator<Item=(K, V)>;
//        fn entries_unboxed(&self) -> ArconResult<Self::Iter> where Self: Sized;

        fn keys<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item=K> + 'a>>;
        // makes it not object-safe :(
//        type KeysIter: Iterator<Item=K>;
//        fn keys_unboxed(&self) -> ArconResult<Self::KeysIter> where Self: Sized;

        fn values<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item=V> + 'a>>;
        // makes it not object-safe :(
//        type ValuesIter: Iterator<Item=V>;
//        fn values_unboxed(&self) -> ArconResult<Self::ValuesIter> where Self: Sized;

        fn is_empty(&self, backend: &SB) -> ArconResult<bool>;
    }

    // analogous to ListState in Flink
    // TODO: Q: Should MergingState::OUT be Vec, or something else? More abstract?
    pub trait VecState<SB, IK, N, T>: MergingState<SB, IK, N, T, Vec<T>> {
        // bikeshed: set / update (Flink)
        fn set(&self, backend: &mut SB, value: Vec<T>) -> ArconResult<()>;
        fn add_all(&self, backend: &mut SB, values: impl IntoIterator<Item=T>) -> ArconResult<()>
            where Self: Sized;
        fn add_all_dyn(&self, backend: &mut SB, values: &mut dyn Iterator<Item=T>) -> ArconResult<()>;
        fn len(&self, backend: &SB) -> ArconResult<usize>;
    }

    pub trait ReducingState<SB, IK, N, T>: MergingState<SB, IK, N, T, T> {}

    pub trait AggregatingState<SB, IK, N, IN, OUT>: MergingState<SB, IK, N, IN, OUT> {}

    // TODO: broadcast state???

    sa::assert_obj_safe!(
        State<(), u32, ()>,
        MapState<(), i32, (), u32, u32>,
        ValueState<(), i32, (), u32>,
        AppendingState<(), i32, (), char, String>,
        MergingState<(), i32, (), u32, std::collections::HashSet<u32>>,
        ReducingState<(), i32, (), u32>,
        VecState<(), i32, (), i32>,
        AggregatingState<(), i32, (), i32, String>
    );
}
