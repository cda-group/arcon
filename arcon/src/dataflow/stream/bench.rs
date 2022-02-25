use super::build::BuildExt;
use crate::stream::operator::sink::measure::MeasureSink;
use crate::{
    application::assembled::AssembledApplication,
    data::ArconType,
    dataflow::stream::OperatorExt,
    dataflow::{
        builder::OperatorBuilder,
        conf::{OperatorConf, ParallelismStrategy},
        stream::Stream,
    },
    index::EmptyState,
};
use std::sync::Arc;

/// Extension trait for benchmarking
pub trait BenchExt: BuildExt {
    /// Adds a final MeasureSink before building an AssembledApplication
    ///
    /// `log_frequency` can be used to tune how often (e.g., every 1000000 events) measurements are logged.
    /// Note: For more stable outputs, use a somewhat large loq frequency > 1000000
    ///
    /// # Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .map(|x| x + 10);
    ///
    /// let log_freq: u64 = 10000000;
    /// let app = stream.measure(log_freq);
    /// ```
    fn measure(self, log_freq: u64) -> AssembledApplication;
}

impl<T: ArconType> BenchExt for Stream<T> {
    #[must_use]
    fn measure(self, log_frequency: u64) -> AssembledApplication {
        let stream = self.operator(OperatorBuilder {
            operator: Arc::new(move || MeasureSink::new(log_frequency)),
            state: Arc::new(|_| EmptyState),
            conf: OperatorConf {
                parallelism_strategy: ParallelismStrategy::Static(1),
                ..Default::default()
            },
        });
        stream.build()
    }
}
