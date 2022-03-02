use crate::application::ApplicationBuilder;
use crate::data::ArconType;
use crate::dataflow::dfg::ChannelKind;
use crate::dataflow::stream::Stream;
use crate::stream::operator::sink::measure::MeasureSink;
use crate::{
    dataflow::stream::OperatorExt,
    dataflow::{
        builder::OperatorBuilder,
        conf::{OperatorConf, ParallelismStrategy},
    },
    index::EmptyState,
};
use std::sync::Arc;

/// Extension trait for sinks
pub trait ToSinkExt<A: ArconType> {
    /// Print the stream outputs
    ///
    /// # Usage
    /// ```no_run
    /// use arcon::prelude::*;
    /// let sink = (0..10u64)
    ///     .to_stream(|conf| _)
    ///     .print();
    /// ```
    fn print(self) -> Sink<A>;
    /// Ignore the stream outputs
    ///
    /// # Usage
    /// ```no_run
    /// use arcon::prelude::*;
    /// let sink = (0..10u64)
    ///     .to_stream(|conf| _)
    ///     .ignore();
    /// ```
    fn ignore(self) -> Sink<A>;
    /// Send stream outputs to a Measure Sink
    ///
    /// # Usage
    /// ```no_run
    /// use arcon::prelude::*;
    /// let sink = (0..10u64)
    ///     .to_stream(|conf| _)
    ///     .measure(10000000);
    /// ```
    fn measure(self, log_freq: u64) -> Sink<A>;
}

pub struct Sink<A: ArconType> {
    stream: Stream<A>,
}

impl<A: ArconType> ToSinkExt<A> for Stream<A> {
    fn print(mut self) -> Sink<A> {
        self.set_channel_kind(ChannelKind::Console);
        Sink { stream: self }
    }
    fn ignore(mut self) -> Sink<A> {
        self.set_channel_kind(ChannelKind::Mute);
        Sink { stream: self }
    }
    fn measure(self, log_freq: u64) -> Sink<A> {
        let mut stream = self.operator(OperatorBuilder {
            operator: Arc::new(move || MeasureSink::new(log_freq)),
            state: Arc::new(|_| EmptyState),
            conf: OperatorConf {
                parallelism_strategy: ParallelismStrategy::Static(1),
                ..Default::default()
            },
        });
        stream.set_channel_kind(ChannelKind::Mute);
        Sink { stream }
    }
}

mod private {
    use super::*;
    pub trait Sealed {}
    impl<A: ArconType> Sealed for Sink<A> {}
}

/// Extension trait for types that can be turned into [ApplicationBuilder]
pub trait ToBuilderExt: private::Sealed {
    fn builder(self) -> ApplicationBuilder;
}

impl<T: ArconType> ToBuilderExt for Sink<T> {
    fn builder(mut self) -> ApplicationBuilder {
        self.stream.move_last_node();
        ApplicationBuilder::new(self.stream.ctx)
    }
}
