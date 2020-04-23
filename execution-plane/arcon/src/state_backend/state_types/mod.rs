// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// TODO: Q: For now this is modelled after Flink. Do we want a different hierarchy, or maybe get
//  rid of the hierarchy altogether?

use crate::state_backend::StateBackend;
use error::ArconResult;
use std::marker::PhantomData;

struct WithDynamicBackend<SB, C>(C, PhantomData<SB>);

// Adds a function that boxes the state and erases the concrete backend type. This is needed for
// building states from dynamic state backends (i.e. we only have StateBackend trait object, not a
// concrete state backend).
macro_rules! erase_backend_type {
    ($t: ident < _ $(, $rest: ident)* >) => {
        // `s` instead of self, because we don't want the method calling syntax for this,
        // because of ambiguities
        fn erase_backend_type(s: Self) -> Box<dyn $t <dyn StateBackend $(, $rest)*> + Send + 'static>
        where
            Self: Sized + Send + 'static,
            SB: StateBackend + Sized,
        {
            Box::new(WithDynamicBackend(s, Default::default()))
        }
    };
}

// different state traits and their hierarchy have been inspired by Flink's State interface hierarchy

//// abstract states ////

/// State from state backend. Can have different values depending on the current key (e.g. the
/// key associated with the current item being processed) and the current namespace (e.g. window
/// index). The existence of these two parameters is heavily inspired by Flink and they may be
/// removed in the future.
///
/// `SB` - state backend type
/// `IK` - key type
/// `N`  - namespace type
pub trait State<SB: ?Sized, IK, N> {
    fn clear(&self, backend: &mut SB) -> ArconResult<()>;

    fn get_current_key(&self) -> ArconResult<&IK>;
    fn set_current_key(&mut self, new_key: IK) -> ArconResult<()>;

    fn get_current_namespace(&self) -> ArconResult<&N>;
    fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()>;

    erase_backend_type!(State<_, IK, N>);
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
pub trait AppendingState<SB: ?Sized, IK, N, IN, OUT>: State<SB, IK, N> {
    fn get(&self, backend: &SB) -> ArconResult<OUT>;
    fn append(&self, backend: &mut SB, value: IN) -> ArconResult<()>;

    erase_backend_type!(AppendingState<_, IK, N, IN, OUT>);
}

pub trait MergingState<SB: ?Sized, IK, N, IN, OUT>: AppendingState<SB, IK, N, IN, OUT> {
    erase_backend_type!(MergingState<_, IK, N, IN, OUT>);
}

//// concrete-ish states ////

pub trait ValueState<SB: ?Sized, IK, N, T>: State<SB, IK, N> {
    fn get(&self, backend: &SB) -> ArconResult<Option<T>>;
    fn set(&self, backend: &mut SB, new_value: T) -> ArconResult<()>;

    erase_backend_type!(ValueState<_, IK, N, T>);
}

pub type BoxedIteratorOfArconResult<'a, T> = Box<dyn Iterator<Item = ArconResult<T>> + 'a>;

pub trait MapState<SB: ?Sized, IK, N, K, V>: State<SB, IK, N> {
    fn get(&self, backend: &SB, key: &K) -> ArconResult<Option<V>>;
    fn fast_insert(&self, backend: &mut SB, key: K, value: V) -> ArconResult<()>;
    fn insert(&self, backend: &mut SB, key: K, value: V) -> ArconResult<Option<V>>;

    /// key_value_pairs must be a finite iterator!
    fn insert_all_dyn(
        &self,
        backend: &mut SB,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()>;
    /// key_value_pairs must be a finite iterator!
    fn insert_all(
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

    fn iter<'a>(&self, backend: &'a SB) -> ArconResult<BoxedIteratorOfArconResult<'a, (K, V)>>;
    fn keys<'a>(&self, backend: &'a SB) -> ArconResult<BoxedIteratorOfArconResult<'a, K>>;
    fn values<'a>(&self, backend: &'a SB) -> ArconResult<BoxedIteratorOfArconResult<'a, V>>;

    fn len(&self, backend: &SB) -> ArconResult<usize>;
    fn is_empty(&self, backend: &SB) -> ArconResult<bool>;

    erase_backend_type!(MapState<_, IK, N, K, V>);
}

// TODO: Q: Should MergingState::OUT be Vec, or something else? More abstract?
// TODO: add more useful methods
pub trait VecState<SB: ?Sized, IK, N, T>: MergingState<SB, IK, N, T, Vec<T>> {
    fn set(&self, backend: &mut SB, value: Vec<T>) -> ArconResult<()>;
    fn add_all(&self, backend: &mut SB, values: impl IntoIterator<Item = T>) -> ArconResult<()>
    where
        Self: Sized;
    fn add_all_dyn(&self, backend: &mut SB, values: &mut dyn Iterator<Item = T>)
        -> ArconResult<()>;
    fn is_empty(&self, backend: &SB) -> ArconResult<bool>;
    fn len(&self, backend: &SB) -> ArconResult<usize>;

    erase_backend_type!(VecState<_, IK, N, T>);
}

pub trait ReducingState<SB: ?Sized, IK, N, T>: MergingState<SB, IK, N, T, Option<T>> {
    erase_backend_type!(ReducingState<_, IK, N, T>);
}

pub trait AggregatingState<SB: ?Sized, IK, N, IN, OUT>: MergingState<SB, IK, N, IN, OUT> {
    erase_backend_type!(AggregatingState<_, IK, N, IN, OUT>);
}

pub trait Aggregator<T> {
    type Accumulator;
    type Result;

    fn create_accumulator(&self) -> Self::Accumulator;
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

static_assertions::assert_obj_safe!(
    State<dyn StateBackend, u32, ()>,
    MapState<dyn StateBackend, i32, (), u32, u32>,
    ValueState<dyn StateBackend, i32, (), u32>,
    AppendingState<dyn StateBackend, i32, (), char, String>,
    MergingState<dyn StateBackend, i32, (), u32, std::collections::HashSet<u32>>,
    ReducingState<dyn StateBackend, i32, (), u32>,
    VecState<dyn StateBackend, i32, (), i32>,
    AggregatingState<dyn StateBackend, i32, (), i32, String>
);

// TODO: inconsistent type parameter order - maybe move `SB, IK, N` to the end of the list in every
//  trait?
pub type BoxedValueState<T, IK = (), N = ()> =
    Box<dyn ValueState<dyn StateBackend, IK, N, T> + Send + 'static>;
pub type BoxedMapState<K, V, IK = (), N = ()> =
    Box<dyn MapState<dyn StateBackend, IK, N, K, V> + Send + 'static>;
pub type BoxedVecState<T, IK = (), N = ()> =
    Box<dyn VecState<dyn StateBackend, IK, N, T> + Send + 'static>;
pub type BoxedReducingState<T, IK = (), N = ()> =
    Box<dyn ReducingState<dyn StateBackend, IK, N, T> + Send + 'static>;
pub type BoxedAggregatingState<IN, OUT, IK = (), N = ()> =
    Box<dyn AggregatingState<dyn StateBackend, IK, N, IN, OUT> + Send + 'static>;

mod boilerplate_impls;
