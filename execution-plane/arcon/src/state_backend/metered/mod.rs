use self::measure_impl::measure;
use crate::{
    prelude::{ArconResult, ValueStateBuilder},
    state_backend::{metered::value_state::MeteredValueState, StateBackend},
};
use static_assertions::_core::ops::Deref;
use std::{any::type_name, cell::RefCell, env, path::Path};

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
    fn measure<T>(&self, operation_name: &'static str, func: impl FnOnce(&SB) -> T) -> T {
        let (res, dp) = measure(operation_name, || func(&self.inner));
        self.metrics.borrow_mut().push(dp);
        res
    }

    fn measure_mut<T>(
        &mut self,
        operation_name: &'static str,
        func: impl FnOnce(&mut SB) -> T,
    ) -> T {
        let (res, dp) = measure(operation_name, || func(&mut self.inner));
        self.metrics.borrow_mut().push(dp);
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

#[cfg(not(feature = "metered_state_backend_rdtsc"))]
mod measure_impl {
    use super::*;
    use once_cell::sync::Lazy;
    use std::time::Instant;

    static ABSOLUTE_START: Lazy<Instant> = Lazy::new(|| Instant::now());

    fn micros_between(start: Instant, end: Instant) -> u64 {
        // the truncation here will return bullshit if the Instants are more than around
        // half a million yrs apart, so this is pretty safe
        end.duration_since(start).as_micros() as u64
    }

    pub fn measure<T>(operation: &'static str, func: impl FnOnce() -> T) -> (T, DataPoint) {
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

#[cfg(feature = "metered_state_backend_rdtsc")]
mod measure_impl {
    use super::*;
    use x86::time::rdtsc;

    pub fn measure<T>(operation: &'static str, func: impl FnOnce() -> T) -> (T, DataPoint) {
        let start = unsafe { rdtsc() };
        let res = func();
        let duration = unsafe { rdtsc() } - start;

        (res, DataPoint {
            operation,
            start,
            duration,
        })
    }
}

pub mod value_state;

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
