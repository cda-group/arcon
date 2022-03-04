use super::builder::SourceBuilderType;
use super::stream::Stream;
use crate::stream::source::Source;
use crate::{data::ArconType, dataflow::conf::DefaultBackend};
use crate::{
    dataflow::{
        builder::SourceBuilder,
        conf::SourceConf,
        constructor::{SourceConstructor, TypedSourceFactory},
        dfg::{DFGNode, DFGNodeKind},
        stream::Context,
    },
    prelude::*,
};
use arcon_state::Backend;
use std::rc::Rc;
use std::sync::Arc;

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "kafka")]
pub use kafka::KafkaSource;

/// Extension trait for types that can be converted to streams
pub trait ToStreamExt<S: ArconType> {
    /// Convert a source type to a [Stream]
    ///
    /// # Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = (0..100u64)
    ///     .to_stream(|conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     });
    /// ```
    fn to_stream<F: FnOnce(&mut SourceConf<S>)>(self, f: F) -> Stream<S>;
}

fn source_to_stream<S, B>(builder_type: SourceBuilderType<S, B>) -> Stream<S::Item>
where
    S: Source,
    B: Backend,
{
    let parallelism = builder_type.parallelism();
    let time = builder_type.time();
    let manager_constructor =
        SourceConstructor::new(String::from("source_manager"), builder_type, time);
    let mut ctx = Context::default();
    let typed_source_factory: Rc<dyn TypedSourceFactory<S::Item>> = Rc::new(manager_constructor);
    let operator_id = 0; // source is the first operator
    let dfg_node = DFGNode::new(DFGNodeKind::Placeholder, operator_id, parallelism, vec![]);
    ctx.dfg.insert(dfg_node);
    Stream::new(ctx, typed_source_factory)
}

impl<I> ToStreamExt<I::Item> for I
where
    I: IntoIterator + 'static + Clone + Send + Sync,
    I::IntoIter: Send,
    I::Item: ArconType,
{
    fn to_stream<F: FnOnce(&mut SourceConf<I::Item>)>(self, f: F) -> Stream<I::Item> {
        let mut conf = SourceConf::default();
        f(&mut conf);
        let builder = SourceBuilder {
            constructor: Arc::new(move |_: Arc<DefaultBackend>| self.clone().into_iter()),
            conf,
        };
        source_to_stream(SourceBuilderType::Single(builder))
    }
}

/// A Local File Source
///
/// Returns a [`Stream`] object that users may execute transformations on.
///
/// # Example
/// ```no_run
/// use arcon::prelude::*;
///
/// let stream: Stream<u64> = LocalFileSource::new("path_to_file")
///     .to_stream(|conf| conf.set_arcon_time(ArconTime::Process));
/// ```
#[derive(Clone)]
pub struct LocalFileSource {
    path: String,
}
impl LocalFileSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }
}

impl<A> ToStreamExt<A> for LocalFileSource
where
    A: ArconType + std::str::FromStr + std::fmt::Display,
    <A as std::str::FromStr>::Err: std::fmt::Display,
{
    fn to_stream<F: FnOnce(&mut SourceConf<A>)>(self, f: F) -> Stream<A> {
        let mut conf = SourceConf::default();
        f(&mut conf);
        let builder = SourceBuilder {
            constructor: Arc::new(move |_: Arc<DefaultBackend>| {
                use crate::stream::source::local_file::LocalFileSourceImpl;
                LocalFileSourceImpl::new(self.path.clone())
            }),
            conf,
        };
        source_to_stream(SourceBuilderType::Single(builder))
    }
}
