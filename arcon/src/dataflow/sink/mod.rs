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
    ///     .to_stream(|conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .print();
    /// ```
    fn print(self) -> Sink<A>;
    /// Ignore the stream outputs
    ///
    /// # Usage
    /// ```no_run
    /// use arcon::prelude::*;
    /// let sink = (0..10u64)
    ///     .to_stream(|conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .ignore();
    /// ```
    fn ignore(self) -> Sink<A>;
    /// Insert the stream outputs to a Debug Node
    ///
    /// # Usage
    /// ```no_run
    /// use arcon::prelude::*;
    /// use std::time::Duration;
    ///
    /// let mut app = (0..10i32)
    ///     .to_stream(|conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .filter(|x| *x < 5)
    ///     .debug()
    ///     .builder()
    ///     .build();
    ///
    /// app.run();
    ///
    /// std::thread::sleep(Duration::from_millis(1000));
    ///
    /// let debug_node = app.get_debug_node::<i32>().unwrap();
    /// debug_node.on_definition(|cd| {
    ///     assert_eq!(cd.data.len(), 5);
    /// });
    /// ```
    fn debug(self) -> Sink<A>;
    /// Send stream outputs to a Measure Sink
    ///
    /// # Usage
    /// ```no_run
    /// use arcon::prelude::*;
    /// let sink = (0..10u64)
    ///     .to_stream(|conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .measure(10000000);
    /// ```
    fn measure(self, log_freq: u64) -> Sink<A>;
}

/// A Sink struct that implements the [ToBuilderExt] trait
///
/// Note that Arcon currently doesn't have a specific Sink trait at this moment.
pub struct Sink<A: ArconType> {
    stream: Stream<A>,
    debug: bool,
}

impl<A: ArconType> ToSinkExt<A> for Stream<A> {
    fn print(mut self) -> Sink<A> {
        self.set_channel_kind(ChannelKind::Console);
        Sink {
            stream: self,
            debug: false,
        }
    }
    fn debug(mut self) -> Sink<A> {
        self.set_channel_kind(ChannelKind::Forward);
        Sink {
            stream: self,
            debug: true,
        }
    }
    fn ignore(mut self) -> Sink<A> {
        self.set_channel_kind(ChannelKind::Mute);
        Sink {
            stream: self,
            debug: false,
        }
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
        Sink {
            stream,
            debug: false,
        }
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
        ApplicationBuilder::new(self.stream.ctx, self.debug)
    }
}
