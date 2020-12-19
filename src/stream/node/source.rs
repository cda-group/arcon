use crate::{
    data::ArconEvent,
    stream::{
        channel::strategy::ChannelStrategy,
        source::{Source, SourceContext, SourceEvent},
    },
    util::SafelySendableFn,
};
use kompact::prelude::*;
use std::cell::RefCell;

#[derive(Debug, Clone, Copy)]
struct ProcessSource;
struct LoopbackPort;
impl Port for LoopbackPort {
    type Indication = Never;
    type Request = ProcessSource;
}

#[derive(ComponentDefinition)]
pub struct SourceNode<S>
where
    S: Source,
{
    /// Component context
    ctx: ComponentContext<Self>,
    loopback_send: RequiredPort<LoopbackPort>,
    loopback_receive: ProvidedPort<LoopbackPort>,
    source: RefCell<S>,
    /// Strategy for outputting events
    channel_strategy: RefCell<ChannelStrategy<S::Data>>,
    /// Timestamp extractor function
    ///
    /// If set to None, timestamps of ArconElement's will also be None.
    ts_extractor: Option<&'static dyn SafelySendableFn(&S::Data) -> u64>,
}

impl<S> SourceNode<S>
where
    S: Source,
{
    pub fn new(source: S, channel_strategy: ChannelStrategy<S::Data>) -> Self {
        Self::setup(source, channel_strategy, None)
    }

    pub fn with_extractor(
        source: S,
        channel_strategy: ChannelStrategy<S::Data>,
        extractor: Option<&'static dyn SafelySendableFn(&S::Data) -> u64>,
    ) -> Self {
        Self::setup(source, channel_strategy, extractor)
    }

    fn setup(
        source: S,
        channel_strategy: ChannelStrategy<S::Data>,
        ts_extractor: Option<&'static dyn SafelySendableFn(&S::Data) -> u64>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            loopback_send: RequiredPort::uninitialised(),
            loopback_receive: ProvidedPort::uninitialised(),
            source: RefCell::new(source),
            channel_strategy: RefCell::new(channel_strategy),
            ts_extractor,
        }
    }

    pub fn handle_source_event(&mut self, event: SourceEvent) {
        match event {
            SourceEvent::Epoch(epoch) => {
                self.channel_strategy
                    .borrow_mut()
                    .add(ArconEvent::Epoch(epoch), self);
            }
            SourceEvent::Watermark(watermark) => {
                self.channel_strategy
                    .borrow_mut()
                    .add(ArconEvent::Watermark(watermark), self);
            }
            SourceEvent::Start => {
                self.loopback_send.trigger(ProcessSource);
            }
        }
    }
}

impl<S> ComponentLifecycle for SourceNode<S>
where
    S: Source,
{
    fn on_start(&mut self) -> Handled {
        let shared = self.loopback_receive.share();
        self.loopback_send.connect(shared);
        Handled::Ok
    }
}

impl<S> Provide<LoopbackPort> for SourceNode<S>
where
    S: Source,
{
    fn handle(&mut self, _event: ProcessSource) -> Handled {
        self.source.borrow_mut().process_batch(SourceContext::new(
            self,
            &mut self.channel_strategy.borrow_mut(),
        ));
        self.loopback_send.trigger(ProcessSource);
        Handled::Ok
    }
}

impl<S> Require<LoopbackPort> for SourceNode<S>
where
    S: Source,
{
    fn handle(&mut self, _event: Never) -> Handled {
        unreachable!("Never type has no instance");
    }
}

impl<S> Actor for SourceNode<S>
where
    S: Source,
{
    type Message = SourceEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.handle_source_event(msg);
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        Handled::Ok
    }
}
