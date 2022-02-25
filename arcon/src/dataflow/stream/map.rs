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

/// Extension trait for map operations
pub trait MapExt<T: ArconType> {
    /// Map each stream record to a possibly new type
    ///
    /// # Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .map(|x| x + 10);
    /// ```
    fn map<OUT: ArconType, F: Fn(T) -> OUT + ArconFnBounds>(self, f: F) -> Stream<OUT>;
    /// Map each record in place keeping the same stream type
    ///
    /// # Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .map_in_place(|x| *x += 10);
    /// ```
    fn map_in_place<F: Fn(&mut T) + ArconFnBounds>(self, f: F) -> Stream<T>;
    /// Akin to [Iterator::flat_map] but on a Stream
    ///
    /// # Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .flat_map(|x| (0..x));
    /// ```
    fn flat_map<I, F>(self, f: F) -> Stream<I::Item>
    where
        I: IntoIterator + 'static,
        I::Item: ArconType,
        F: Fn(T) -> I + ArconFnBounds;
}

impl<T: ArconType> MapExt<T> for Stream<T> {
    #[must_use]
    fn map<OUT: ArconType, F: Fn(T) -> OUT + ArconFnBounds>(self, f: F) -> Stream<OUT> {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || function::Map::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
    }
    #[must_use]
    fn map_in_place<F: Fn(&mut T) + ArconFnBounds>(self, f: F) -> Stream<T> {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || function::MapInPlace::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
    }
    #[must_use]
    fn flat_map<I, F>(self, f: F) -> Stream<I::Item>
    where
        I: IntoIterator + 'static,
        I::Item: ArconType,
        F: Fn(T) -> I + ArconFnBounds,
    {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || function::FlatMap::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
    }
}
