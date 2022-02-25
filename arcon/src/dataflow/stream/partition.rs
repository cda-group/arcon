use crate::data::ArconType;
use crate::dataflow::stream::{KeyBuilder, KeyedStream, Stream};
use crate::util::ArconFnBounds;
use std::{hash::Hash, hash::Hasher, rc::Rc, sync::Arc};

/// Extension trait for partitioning schemes
pub trait PartitionExt<T: ArconType> {
    /// Consistently partition the Stream using the given key extractor method.
    ///
    /// The key extractor function must be deterministic, for two identical events it
    /// must return the same key whenever it is called.
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: KeyedStream<u64> = Application::default()
    ///     .iterator(0u64..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .key_by(|i: &u64| i);
    /// ```
    fn key_by<KEY: Hash + 'static, F: Fn(&T) -> &KEY + ArconFnBounds>(
        self,
        key_extractor: F,
    ) -> KeyedStream<T>;
}

impl<T: ArconType> PartitionExt<T> for Stream<T> {
    #[must_use]
    fn key_by<KEY: Hash + 'static, F: Fn(&T) -> &KEY + ArconFnBounds>(
        mut self,
        key_extractor: F,
    ) -> KeyedStream<T> {
        let key_builder = KeyBuilder {
            extractor: Arc::new(move |d: &T| {
                let mut hasher = arcon_util::key_hasher();
                key_extractor(d).hash(&mut hasher);
                hasher.finish()
            }),
        };
        if let Some(ref mut node_factory) = self.last_node {
            let node_factory = Rc::get_mut(node_factory).unwrap();
            node_factory.set_key_builder(key_builder.clone());
            self.key_builder = Some(key_builder);
        } else if let Some(ref mut source_factory) = self.source {
            let source_factory = Rc::get_mut(source_factory).unwrap();
            source_factory.set_key_builder(key_builder.clone());
            self.key_builder = Some(key_builder);
        } else {
            panic!("Nothing to apply key_by on!");
        }
        KeyedStream::from(self)
    }
}
