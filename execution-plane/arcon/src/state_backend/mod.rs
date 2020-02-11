// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate static_assertions as sa;
#[cfg(test)]
extern crate tempfile;

use arcon_error::ArconResult;
use state_types::*;

// NOTE: we are using bincode for serialization, so it's probably not portable between architectures
// of different endianness

// TODO: a lot of params here could be borrows
// TODO: figure out if this needs to be Send + Sync
/// Trait required for all state backend implementations in Arcon
pub trait StateBackend {
    fn new(path: &str) -> ArconResult<Self>
    where
        Self: Sized;

    fn checkpoint(&self, checkpoint_path: &str) -> ArconResult<()>;
    fn restore(restore_path: &str, checkpoint_path: &str) -> ArconResult<Self>
    where
        Self: Sized;
}

//// builders ////
// ideally this would be a part of the StateBackend trait, but we lack generic associated types
pub trait ValueStateBuilder<IK, N, T, KS, TS>: Sized {
    type Type: ValueState<Self, IK, N, T, KS, TS>;
    fn new_value_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

pub trait MapStateBuilder<IK, N, K, V, KS, TS>: Sized {
    type Type: MapState<Self, IK, N, K, V, KS, TS>;
    fn new_map_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

pub trait VecStateBuilder<IK, N, T, KS, TS>: Sized {
    type Type: VecState<Self, IK, N, T, KS, TS>;
    fn new_vec_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

pub trait ReducingStateBuilder<IK, N, T, F, KS, TS>: Sized {
    type Type: ReducingState<Self, IK, N, T, KS, TS>;
    fn new_reducing_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        reduce_fn: F,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

pub trait AggregatingStateBuilder<IK, N, T, AGG: Aggregator<T>, KS, TS>: Sized {
    type Type: AggregatingState<Self, IK, N, T, AGG::Result, KS, TS>;
    fn new_aggregating_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        aggregator: AGG,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

#[macro_use]
mod state_types {
    // TODO: Q: Should methods that mutate the state actually take a mutable reference to self?
    // TODO: Q: For now this is modelled after Flink. Do we want a different hierarchy, or maybe get
    //  rid of the hierarchy altogether?

    use super::*;
    use crate::state_backend::serialization::SerializableFixedSizeWith;

    //// abstract states ////
    /// State inside a stream.
    ///
    /// `SB` - state backend type
    /// `IK` - type of key of the item currently in the stream
    /// `N`  - type of the namespace
    /// `KS` - type of the key serializer (actually item key, namespace, and user key)
    /// `TS` - type of the value serializer
    pub trait State<SB, IK, N, KS, TS> {
        fn clear(&self, backend: &mut SB) -> ArconResult<()>;

        fn get_current_key(&self) -> ArconResult<&IK>;
        fn set_current_key(&mut self, new_key: IK) -> ArconResult<()>;

        fn get_current_namespace(&self) -> ArconResult<&N>;
        fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()>;
    }

    macro_rules! delegate_key_and_namespace {
        ($common: ident) => {
            fn get_current_key(&self) -> ArconResult<&IK> {
                Ok(&self.$common.item_key)
            }

            fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
                self.$common.item_key = new_key;
                Ok(())
            }

            fn get_current_namespace(&self) -> ArconResult<&N> {
                Ok(&self.$common.namespace)
            }

            fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
                self.$common.namespace = new_namespace;
                Ok(())
            }
        };
    }

    // TODO: since we don't have any state that is appending, but not merging, maybe consider using one trait?
    pub trait AppendingState<SB, IK, N, IN, OUT, KS, TS>: State<SB, IK, N, KS, TS> {
        fn get(&self, backend: &SB) -> ArconResult<OUT>;
        fn append(&self, backend: &mut SB, value: IN) -> ArconResult<()>;
    }

    pub trait MergingState<SB, IK, N, IN, OUT, KS, TS>:
        AppendingState<SB, IK, N, IN, OUT, KS, TS>
    {
    }

    //// concrete-ish states ////

    pub trait ValueState<SB, IK, N, T, KS, TS>: State<SB, IK, N, KS, TS> {
        // bikeshed: get / value (Flink)
        fn get(&self, backend: &SB) -> ArconResult<T>;

        // bikeshed: set / update (Flink)
        fn set(&self, backend: &mut SB, new_value: T) -> ArconResult<()>;
    }

    pub trait MapState<SB, IK, N, K, V, KS, TS>: State<SB, IK, N, KS, TS> {
        fn get(&self, backend: &SB, key: &K) -> ArconResult<V>;
        fn put(&self, backend: &mut SB, key: K, value: V) -> ArconResult<()>;

        /// key_value_pairs must be a finite iterator!
        fn put_all_dyn(
            &self,
            backend: &mut SB,
            key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
        ) -> ArconResult<()>;
        /// key_value_pairs must be a finite iterator!
        fn put_all(
            &self,
            backend: &mut SB,
            key_value_pairs: impl IntoIterator<Item = (K, V)>,
        ) -> ArconResult<()>
        where
            Self: Sized;

        fn remove(&self, backend: &mut SB, key: &K) -> ArconResult<()>;
        fn contains(&self, backend: &SB, key: &K) -> ArconResult<bool>;

        // unboxed iterators would require associated types generic over backend's lifetime
        // TODO: impl this when GATs land on nightly

        fn iter<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item = (K, V)> + 'a>>;
        fn keys<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item = K> + 'a>>;
        fn values<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item = V> + 'a>>;

        fn is_empty(&self, backend: &SB) -> ArconResult<bool>;
    }

    // analogous to ListState in Flink
    // TODO: Q: Should MergingState::OUT be Vec, or something else? More abstract?
    pub trait VecState<SB, IK, N, T, KS, TS>: MergingState<SB, IK, N, T, Vec<T>, KS, TS> {
        // bikeshed: set / update (Flink)
        fn set(&self, backend: &mut SB, value: Vec<T>) -> ArconResult<()>;
        fn add_all(&self, backend: &mut SB, values: impl IntoIterator<Item = T>) -> ArconResult<()>
        where
            Self: Sized;
        fn add_all_dyn(
            &self,
            backend: &mut SB,
            values: &mut dyn Iterator<Item = T>,
        ) -> ArconResult<()>;

        fn len(&self, backend: &SB) -> ArconResult<usize>
        where
            T: SerializableFixedSizeWith<TS>; // can be problematic
    }

    pub trait ReducingState<SB, IK, N, T, KS, TS>: MergingState<SB, IK, N, T, T, KS, TS> {}

    pub trait AggregatingState<SB, IK, N, IN, OUT, KS, TS>:
        MergingState<SB, IK, N, IN, OUT, KS, TS>
    {
    }

    pub trait Aggregator<T> {
        type Accumulator;
        type Result;

        fn create_accumulator(&self) -> Self::Accumulator;
        // bikeshed - immutable + return value instead of mutable acc? (like in Flink)
        fn add(&self, acc: &mut Self::Accumulator, value: T);
        fn merge_accumulators(
            &self,
            fst: Self::Accumulator,
            snd: Self::Accumulator,
        ) -> Self::Accumulator;
        fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result;
    }

    #[derive(Clone)]
    pub struct ClosuresAggregator<CREATE, ADD, MERGE, RES> {
        create: CREATE,
        add: ADD,
        merge: MERGE,
        res: RES,
    }

    impl<CREATE, ADD, MERGE, RES> ClosuresAggregator<CREATE, ADD, MERGE, RES> {
        #[allow(dead_code)] // used by tests
        pub fn new<T, ACC, R>(
            create: CREATE,
            add: ADD,
            merge: MERGE,
            res: RES,
        ) -> ClosuresAggregator<CREATE, ADD, MERGE, RES>
        where
            CREATE: Fn() -> ACC,
            ADD: Fn(&mut ACC, T) -> (),
            RES: Fn(ACC) -> R,
            MERGE: Fn(ACC, ACC) -> ACC,
        {
            ClosuresAggregator {
                create,
                add,
                merge,
                res,
            }
        }
    }

    impl<CREATE, ADD, MERGE, RES, T> Aggregator<T> for ClosuresAggregator<CREATE, ADD, MERGE, RES>
    where
        CREATE: Fn<()>,
        ADD: Fn(&mut CREATE::Output, T) -> (),
        RES: Fn<(CREATE::Output,)>,
        MERGE: Fn(CREATE::Output, CREATE::Output) -> CREATE::Output,
    {
        type Accumulator = CREATE::Output;
        type Result = RES::Output;

        fn create_accumulator(&self) -> Self::Accumulator {
            (self.create)()
        }

        fn add(&self, acc: &mut Self::Accumulator, value: T) {
            (self.add)(acc, value)
        }

        fn merge_accumulators(
            &self,
            fst: Self::Accumulator,
            snd: Self::Accumulator,
        ) -> Self::Accumulator {
            (self.merge)(fst, snd)
        }

        fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result {
            (self.res)(acc)
        }
    }

    // TODO: broadcast state???

    // let's not care about object safety
    //    sa::assert_obj_safe!(
    //        State<(), u32, ()>,
    //        MapState<(), i32, (), u32, u32>,
    //        ValueState<(), i32, (), u32>,
    //        AppendingState<(), i32, (), char, String>,
    //        MergingState<(), i32, (), u32, std::collections::HashSet<u32>>,
    //        ReducingState<(), i32, (), u32>,
    //        VecState<(), i32, (), i32>,
    //        AggregatingState<(), i32, (), i32, String>
    //    );
}

pub mod serialization;

pub mod in_memory; // TODO: fix serialization
#[cfg(feature = "arcon_rocksdb")]
pub mod rocksdb;
