use crate::{
    error::*, Aggregator, AggregatorState, Backend, Config, Handle, Key, MapState, Metakey,
    Reducer, ReducerState, Session, Value, ValueState, VecState,
};
use custom_debug::CustomDebug;
use once_cell::sync::Lazy;
use std::{
    any::type_name, cell::RefCell, collections::HashMap, env, ops::Deref, path::Path, time::Instant,
};

#[derive(Clone, Debug, Default)]
pub struct DataPoint {
    operation: &'static str,
    // the fields below aren't the std::time::{Instant,Duration} types just so we can save a bit
    // of space
    start: u64,
    duration: u64,
}

/// Collection of datapoints. When full it'll start overwriting the oldest entries
#[derive(CustomDebug)]
pub struct Metrics {
    num_pushed: usize,
    #[debug(skip)]
    storage: Box<[DataPoint]>,
}

impl Metrics {
    fn new(cap: usize) -> Metrics {
        Metrics {
            num_pushed: 0,
            storage: vec![DataPoint::default(); cap].into_boxed_slice(),
        }
    }

    fn push(&mut self, dp: DataPoint) {
        self.storage[self.num_pushed % self.storage.len()] = dp;
        self.num_pushed += 1;
    }

    pub fn summary(&self) -> String {
        struct Summary {
            count: u64,
            min: u64,
            avg: u64,
            max: u64,
        }

        let map = self.iter().fold(HashMap::new(), |mut acc, dp| {
            let Summary {
                count,
                min,
                avg,
                max,
            } = acc.entry(dp.operation).or_insert_with(|| Summary {
                count: 0,
                min: u64::max_value(),
                avg: 0,
                max: 0,
            });

            *min = (*min).min(dp.duration);
            *max = (*max).max(dp.duration);
            *avg = (*avg * *count + dp.duration) / (*count + 1);
            *count += 1;

            acc
        });

        let mut res = format!("{:20}\tcount\tmin\tavg\tmax", "operation");
        for (
            operation,
            Summary {
                count,
                min,
                avg,
                max,
            },
        ) in map
        {
            use std::fmt::Write;
            write!(
                res,
                "\n{operation:20}\t{count}\t{min}\t{avg}\t{max}",
                operation = operation,
                count = count,
                min = min,
                avg = avg,
                max = max
            )
            .expect("Could not write to the result String")
        }

        res
    }
}

impl Deref for Metrics {
    type Target = [DataPoint];

    fn deref(&self) -> &Self::Target {
        if self.num_pushed > self.storage.len() {
            &self.storage
        } else {
            &self.storage[0..self.num_pushed]
        }
    }
}

#[derive(Debug)]
pub struct Metered<SB> {
    inner: SB,
    pub backend_name: &'static str,
    pub metrics: RefCell<Metrics>,
}

impl<B: Backend> Metered<B> {
    fn new_from_initial_measurement((inner, dp): (Result<B>, DataPoint)) -> Result<Metered<B>> {
        let inner = inner?;
        let metrics_cap = env::var("ARCON_SB_METRICS_LEN")
            .map_err(|_| ())
            .and_then(|s| s.parse::<usize>().map_err(|_| ()))
            .unwrap_or(DEFAULT_METRICS_CAP);

        let mut metrics = Metrics::new(metrics_cap);
        metrics.push(dp);
        let metrics = RefCell::new(metrics);

        let backend_name = type_name::<B>();

        Ok(Metered {
            inner,
            backend_name,
            metrics,
        })
    }

    fn measure<'a, T>(&'a self, operation_name: &'static str, func: impl FnOnce(&'a B) -> T) -> T {
        let (res, dp) = measure(operation_name, || func(&self.inner));
        self.metrics.borrow_mut().push(dp);
        res
    }

    fn measure_mut<'a, T>(
        &'a mut self,
        operation_name: &'static str,
        func: impl FnOnce(&'a mut B) -> T,
    ) -> T {
        let inner: &'a mut B = &mut self.inner;
        let metrics = &mut self.metrics;
        let (res, dp) = measure(operation_name, move || func(inner));
        metrics.get_mut().push(dp);
        res
    }
}

const DEFAULT_METRICS_CAP: usize = {
    const MB: usize = 2usize << 20;
    10 * MB / std::mem::size_of::<DataPoint>()
};

static ABSOLUTE_START: Lazy<Instant> = Lazy::new(|| Instant::now());

fn nanos_between(start: Instant, end: Instant) -> u64 {
    // the truncation here will return bullshit if the Instants are more than around
    // half a million yrs apart, so this is pretty safe
    end.duration_since(start).as_nanos() as u64
}

fn measure<T>(operation: &'static str, func: impl FnOnce() -> T) -> (T, DataPoint) {
    // we have to evaluate this before `start` in case the lazy cell isn't initialized yet
    let abs_start = *ABSOLUTE_START;

    let start = Instant::now();
    let res = func();
    let duration = nanos_between(start, Instant::now());

    (res, DataPoint {
        operation,
        start: nanos_between(abs_start, start),
        duration,
    })
}

impl<B: Backend + 'static> Backend for Metered<B> {
    fn restore_or_create(config: &Config, id: String) -> Result<Self>
    where
        Self: Sized,
    {
        Self::new_from_initial_measurement(measure("Backend::restore_or_create", || {
            B::restore_or_create(config, id)
        }))
    }

    fn create(live_path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        Self::new_from_initial_measurement(measure("Backend::create", || B::create(live_path)))
    }

    fn restore(live_path: &Path, checkpoint_path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        Self::new_from_initial_measurement(measure("Backend::restore", || {
            B::restore(live_path, checkpoint_path)
        }))
    }

    fn was_restored(&self) -> bool {
        self.inner.was_restored()
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        self.measure("Backend::checkpoint", |backend| {
            backend.checkpoint(checkpoint_path)
        })
    }

    fn session(&mut self) -> Session<Self> {
        // we need to trigger inner session creation
        let mut session = self.inner.session();
        // but we steal its drop hook just to fire it later
        let inner_drop_hook = session.drop_hook.take();
        // we drop the inner session, _but it isn't actually closed_, it has an empty drop hook now
        drop(session);

        Session {
            backend: self,
            drop_hook: inner_drop_hook.map(|dh| {
                Box::new(move |this: &mut Self| dh(&mut this.inner)) as Box<dyn FnOnce(&mut Self)>
            }),
        }
    }

    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        self.measure_mut("Backend::register_value_handle", |backend| {
            backend.register_value_handle(handle)
        })
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        self.measure_mut("Backend::register_map_handle", |backend| {
            backend.register_map_handle(handle)
        })
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        self.measure_mut("Backend::register_vec_handle", |backend| {
            backend.register_vec_handle(handle)
        })
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        self.measure_mut("Backend::register_reducer_handle", |backend| {
            backend.register_reducer_handle(handle)
        })
    }

    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) {
        self.measure_mut("Backend::register_aggregator_handle", |backend| {
            backend.register_aggregator_handle(handle)
        })
    }
}

macro_rules! measure_delegated {
    ($ops_type:ident :) => {};
    ($ops_type:ident : fn $fn_name:ident $(<
        $($generic_param:ident $(: $first_bound:path $(: $other_bounds:path)*)?),*$(,)?
    >)? (
        &self, $($rest_name:ident : $rest_ty:ty),* $(,)?
    ) $(-> $return_type:ty)?; $($other_decl:tt)*) => {
        fn $fn_name $(<
            $($generic_param $(: $first_bound $(+ $other_bounds)*)?),*
        >)? (
            &self, $($rest_name: $rest_ty),*
        ) $(-> $return_type)? {
            self.measure(
                concat!(stringify!($ops_type), "::", stringify!($fn_name)),
                move |b| b.$fn_name($($rest_name),*)
            )
        }

        measure_delegated!($ops_type : $($other_decl)*);
    };
    ($ops_type:ident : fn $fn_name:ident $(<
        $($generic_param:ident $(: $first_bound:path $(: $other_bounds:path)*)?),*$(,)?
    >)? (
        &mut self, $($rest_name:ident : $rest_ty:ty),* $(,)?
    ) $(-> $return_type:ty)?; $($other_decl:tt)*) => {
        fn $fn_name $(<
            $($generic_param $(: $first_bound $(+ $other_bounds)*)?),*
        >)? (
            &mut self, $($rest_name: $rest_ty),*
        ) $(-> $return_type)? {
            self.measure_mut(
                concat!(stringify!($ops_type), "::", stringify!($fn_name)),
                move |b| b.$fn_name($($rest_name),*)
            )
        }

        measure_delegated!($ops_type : $($other_decl)*);
    };
}

mod aggregator_ops;
mod map_ops;
mod reducer_ops;
mod value_ops;
mod vec_ops;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemory;
    use std::path::PathBuf;

    fn backend() -> Metered<InMemory> {
        Metered::<InMemory>::create(&PathBuf::new()).unwrap()
    }

    common_state_tests!(backend());
}
