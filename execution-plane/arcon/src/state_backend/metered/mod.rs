use self::measure_impl::measure;
use crate::{prelude::ArconResult, state_backend::StateBackend};
use std::{any::type_name, cell::RefCell, convert::TryInto, env, path::Path};

#[derive(Copy, Clone, Debug, Default)]
pub struct DataPoint {
    operation: &'static str,
    start: u32,
    duration: u32,
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

impl<SB> Metered<SB> {
    fn measure<T>(&self, operation_name: &'static str, func: impl Fn() -> T) -> T {
        let (res, dp) = measure(operation_name, func);
        self.metrics.borrow_mut().push(dp);
        res
    }
}

// four megs worth of data points - DataPoint is 24 bytes, so this is around 175_000 DPs
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
        let (inner, dp) = measure("new", || SB::new(path));
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
        self.measure("checkpoint", || self.inner.checkpoint(checkpoint_path))
    }

    fn restore(restore_path: &Path, checkpoint_path: &Path) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let (inner, dp) = measure("restore", || SB::restore(restore_path, checkpoint_path));
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

    /// saturates if the difference between the times is greater than around 70 minutes
    fn micros_between_saturating(start: Instant, end: Instant) -> u32 {
        end.duration_since(start)
            .as_micros()
            .try_into()
            .unwrap_or(u32::max_value())
    }

    pub fn measure<T>(operation: &'static str, func: impl Fn() -> T) -> (T, DataPoint) {
        let start = Instant::now();
        let res = func();
        let duration = micros_between_saturating(start, Instant::now());

        (res, DataPoint {
            operation,
            start: micros_between_saturating(*ABSOLUTE_START, start),
            duration,
        })
    }
}

#[cfg(feature = "metered_state_backend_rdtsc")]
mod measure_impl {
    use super::*;
    use x86::time::rdtsc;

    pub fn measure<T>(operation: &'static str, func: impl Fn() -> T) -> (T, DataPoint) {
        let start = unsafe { rdtsc() };
        let res = func();
        let duration = (unsafe { rdtsc() } - start)
            .try_into()
            .unwrap_or(u32::max_value());

        (res, DataPoint {
            operation,
            start: start.try_into().unwrap_or(u32::max_value()),
            duration,
        })
    }
}
