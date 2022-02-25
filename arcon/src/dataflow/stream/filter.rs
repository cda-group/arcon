use crate::{
    data::ArconType,
    dataflow::{
        builder::OperatorBuilder,
        stream::{OperatorExt, Stream},
    },
    index::EmptyState,
    stream::operator::function,
    util::ArconFnBounds,
};
use std::sync::Arc;

/// Extension trait for filter operations
pub trait FilterExt<T: ArconType> {
    /// Filter out records based on the given predicate
    ///
    /// # Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .filter(|x| x < &50);
    /// ```
    fn filter<F: Fn(&T) -> bool + ArconFnBounds>(self, f: F) -> Self;
}

impl<T: ArconType> FilterExt<T> for Stream<T> {
    #[must_use]
    fn filter<F: Fn(&T) -> bool + ArconFnBounds>(self, f: F) -> Self {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || function::Filter::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
    }
}
