// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::{AggregatingStateBuilder, ArconResult},
    state_backend::{
        metered::{
            aggregating_state::MeteredAggregatingState, map_state::MeteredMapState,
            reducing_state::MeteredReducingState, value_state::MeteredValueState,
            vec_state::MeteredVecState,
        },
        state_types::Aggregator,
        MapStateBuilder, ReducingStateBuilder, StateBackend, ValueStateBuilder, VecStateBuilder,
    },
};
use cfg_if::cfg_if;
use std::{any::type_name, cell::RefCell, env, ops::Deref, path::Path};

#[derive(Clone, Debug, Default)]
pub struct DataPoint {
    operation: &'static str,
    // so, like, those _could_ be u32, but that only shaves off 8 bytes (padding) and would
    // make the logic slightly more complicated when using rdtsc
    start: u64,
    duration: u64,
}

pub struct Metrics(usize, Box<[DataPoint]>);

pub struct Metered<SB> {
    inner: SB,
    backend_name: &'static str,
    metrics: RefCell<Metrics>,
}

impl Metrics {
    fn new(cap: usize) -> Metrics {
        Metrics(0, vec![DataPoint::default(); cap].into_boxed_slice())
    }

    fn push(&mut self, dp: DataPoint) {
        assert!(self.0 < self.1.len());
        self.1[self.0] = dp;
        self.0 += 1;
    }
}

impl Deref for Metrics {
    type Target = [DataPoint];

    fn deref(&self) -> &Self::Target {
        &self.1[0..self.0]
    }
}

impl<SB> Metered<SB> {
    fn measure<'a, T>(&'a self, operation_name: &'static str, func: impl FnOnce(&'a SB) -> T) -> T {
        let (res, dp) = measure(operation_name, || func(&self.inner));
        self.metrics.borrow_mut().push(dp);
        res
    }

    fn measure_mut<'a, T>(
        &'a mut self,
        operation_name: &'static str,
        func: impl FnOnce(&'a mut SB) -> T,
    ) -> T {
        let inner: &'a mut SB = &mut self.inner;
        let metrics = &mut self.metrics;
        let (res, dp) = measure(operation_name, move || func(inner));
        metrics.get_mut().push(dp);
        res
    }
}

// four megs worth of data points - DataPoint is 32 bytes, so this is around 130k DPs
const DEFAULT_METRICS_CAP: usize = {
    const MB: usize = 2usize << 20;
    4 * MB / std::mem::size_of::<DataPoint>()
};

impl<SB> StateBackend for Metered<SB>
where
    SB: StateBackend,
{
    fn new(path: &Path) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let (inner, dp) = measure("StateBackend::new", || SB::new(path));
        let inner = inner?;
        let metrics_cap = env::var("ARCON_SB_METRICS_LEN")
            .map_err(|_| ())
            .and_then(|s| s.parse::<usize>().map_err(|_| ()))
            .unwrap_or(DEFAULT_METRICS_CAP);

        let mut metrics = Metrics::new(metrics_cap);
        metrics.push(dp);
        let metrics = RefCell::new(metrics);

        let backend_name = type_name::<SB>();

        Ok(Metered {
            inner,
            backend_name,
            metrics,
        })
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> ArconResult<()> {
        self.measure("StateBackend::checkpoint", |backend| {
            backend.checkpoint(checkpoint_path)
        })
    }

    fn restore(restore_path: &Path, checkpoint_path: &Path) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let (inner, dp) = measure("StateBackend::restore", || {
            SB::restore(restore_path, checkpoint_path)
        });
        let inner = inner?;
        let metrics_cap = env::var("ARCON_SB_METRICS_LEN")
            .map_err(|_| ())
            .and_then(|s| s.parse::<usize>().map_err(|_| ()))
            .unwrap_or(DEFAULT_METRICS_CAP);

        let mut metrics = Metrics::new(metrics_cap);
        metrics.push(dp);
        let metrics = RefCell::new(metrics);

        let backend_name = type_name::<SB>();

        Ok(Metered {
            inner,
            backend_name,
            metrics,
        })
    }

    fn was_restored(&self) -> bool {
        self.inner.was_restored()
    }
}

cfg_if! {
    if #[cfg(all(
        feature = "metered_state_backend_rdtsc",
        any(target_arch = "x86", target_arch = "x86_64")
    ))] {
        use x86::time::rdtsc;

        fn measure<T>(operation: &'static str, func: impl FnOnce() -> T) -> (T, DataPoint) {
            let start = unsafe { rdtsc() };
            let res = func();
            let duration = unsafe { rdtsc() } - start;

            (res, DataPoint {
                operation,
                start,
                duration,
            })
        }
    } else {
        use once_cell::sync::Lazy;
        use std::time::Instant;

        static ABSOLUTE_START: Lazy<Instant> = Lazy::new(|| Instant::now());

        fn micros_between(start: Instant, end: Instant) -> u64 {
            // the truncation here will return bullshit if the Instants are more than around
            // half a million yrs apart, so this is pretty safe
            end.duration_since(start).as_micros() as u64
        }

        fn measure<T>(operation: &'static str, func: impl FnOnce() -> T) -> (T, DataPoint) {
            let start = Instant::now();
            let res = func();
            let duration = micros_between(start, Instant::now());

            (res, DataPoint {
                operation,
                start: micros_between(*ABSOLUTE_START, start),
                duration,
            })
        }
    }
}

macro_rules! impl_metered_state {
    ($state_name:ident <$param:ident> : $state_type:ident) => {
        impl<$param, SB, IK, N> State<Metered<SB>, IK, N> for $state_name<$param>
        where
            $param: State<SB, IK, N>,
        {
            fn clear(&self, backend: &mut Metered<SB>) -> ArconResult<()> {
                backend.measure_mut(concat!(stringify!($state_type), "::clear"), |backend| {
                    self.inner.clear(backend)
                })
            }

            fn get_current_key(&self) -> ArconResult<&IK> {
                self.inner.get_current_key()
            }

            fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
                self.inner.set_current_key(new_key)
            }

            fn get_current_namespace(&self) -> ArconResult<&N> {
                self.inner.get_current_namespace()
            }

            fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
                self.inner.set_current_namespace(new_namespace)
            }
        }
    };
}

macro_rules! measure_delegated {
    ($state_type:ident :) => {};
    ($state_type:ident : fn $fn_name:ident $(<$($generics:tt),*>)? (
        &self, backend: &Metered<SB> $(, $($rest_name:ident : $rest_ty:ty),*)? $(,)?
    ) $(-> $return_type:ty)? $(where Self: $self_bound:ident)? $(,)?; $($other_decl:tt)*) => {
        fn $fn_name $(<$($generics:tt),*>)? (
            &self, backend: &Metered<SB>, $($($rest_name: $rest_ty),*)?
        ) $(-> $return_type)? $(where Self: $self_bound)? {
            backend.measure(
                concat!(stringify!($state_type), "::", stringify!($fn_name)),
                move |b| self.inner.$fn_name(b, $($($rest_name),*)?)
            )
        }

        measure_delegated!($state_type : $($other_decl)*);
    };
    ($state_type:ident : fn $fn_name:ident $(<$($generics:tt),*>)? (
        &self, backend: &mut Metered<SB> $(, $($rest_name:ident : $rest_ty:ty),*)? $(,)?
    ) $(-> $return_type:ty)? $(where Self: $self_bound:ident)? $(,)?; $($other_decl:tt)*) => {
        fn $fn_name $(<$($generics:tt),*>)? (
            &self, backend: &mut Metered<SB>, $($($rest_name: $rest_ty),*)?
        ) $(-> $return_type)? $(where Self: $self_bound)? {
            backend.measure_mut(
                concat!(stringify!($state_type), "::", stringify!($fn_name)),
                move |b| self.inner.$fn_name(b, $($($rest_name),*)?)
            )
        }

        measure_delegated!($state_type : $($other_decl)*);
    };
}

pub mod aggregating_state;
pub mod map_state;
pub mod reducing_state;
pub mod value_state;
pub mod vec_state;

impl<SB, IK, N, T, KS, TS> ValueStateBuilder<IK, N, T, KS, TS> for Metered<SB>
where
    SB: ValueStateBuilder<IK, N, T, KS, TS>,
{
    type Type = MeteredValueState<SB::Type>;

    fn new_value_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let inner = self.measure_mut("ValueStateBuilder::new_value_state", move |backend| {
            backend.new_value_state(name, item_key, namespace, key_serializer, value_serializer)
        });

        MeteredValueState { inner }
    }
}

impl<SB, IK, N, K, V, KS, TS> MapStateBuilder<IK, N, K, V, KS, TS> for Metered<SB>
where
    SB: MapStateBuilder<IK, N, K, V, KS, TS>,
    K: 'static,
    V: 'static,
{
    type Type = MeteredMapState<SB::Type>;

    fn new_map_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let inner = self.measure_mut("MapStateBuilder::new_map_state", move |backend| {
            backend.new_map_state(name, item_key, namespace, key_serializer, value_serializer)
        });

        MeteredMapState { inner }
    }
}

impl<SB, IK, N, T, KS, TS> VecStateBuilder<IK, N, T, KS, TS> for Metered<SB>
where
    SB: VecStateBuilder<IK, N, T, KS, TS>,
{
    type Type = MeteredVecState<SB::Type>;

    fn new_vec_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let inner = self.measure_mut("VecStateBuilder::new_vec_state", move |backend| {
            backend.new_vec_state(name, item_key, namespace, key_serializer, value_serializer)
        });

        MeteredVecState { inner }
    }
}

impl<SB, IK, N, T, F, KS, TS> ReducingStateBuilder<IK, N, T, F, KS, TS> for Metered<SB>
where
    SB: ReducingStateBuilder<IK, N, T, F, KS, TS>,
{
    type Type = MeteredReducingState<SB::Type>;

    fn new_reducing_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        reduce_fn: F,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let inner = self.measure_mut("ReducingStateBuilder::new_reducing_state", move |backend| {
            backend.new_reducing_state(
                name,
                item_key,
                namespace,
                reduce_fn,
                key_serializer,
                value_serializer,
            )
        });

        MeteredReducingState { inner }
    }
}

impl<SB, IK, N, T, AGG, KS, TS> AggregatingStateBuilder<IK, N, T, AGG, KS, TS> for Metered<SB>
where
    SB: AggregatingStateBuilder<IK, N, T, AGG, KS, TS>,
    AGG: Aggregator<T>,
{
    type Type = MeteredAggregatingState<SB::Type>;

    fn new_aggregating_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        aggregator: AGG,
        key_serializer: KS,
        _value_serializer: TS,
    ) -> Self::Type {
        let inner = self.measure_mut(
            "AggregatingStateBuilder::new_aggregating_state",
            move |backend| {
                backend.new_aggregating_state(
                    name,
                    item_key,
                    namespace,
                    aggregator,
                    key_serializer,
                    _value_serializer,
                )
            },
        );

        MeteredAggregatingState { inner }
    }
}
