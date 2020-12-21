// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, ArconType},
    stream::channel::strategy::ChannelStrategy,
    util::get_system_time,
};
use kompact::prelude::ComponentDefinition;

pub mod collection;
pub mod local_file;
//#[cfg(feature = "kafka")]
//pub mod kafka;
//#[cfg(feature = "socket")]
//pub mod socket;

/// Defines an Arcon Source and the methods it must implement
pub trait Source: Send + Sized + 'static {
    /// The type of data produced by the Source
    type Data: ArconType;

    /// Process a batch of source data
    ///
    /// Safety: This method must be non-blocking
    fn process_batch(&self, ctx: SourceContext<Self, impl ComponentDefinition>);

    /// Define how the source should extract timestamps
    fn extract_timestamp(&self, data: &Self::Data) -> Option<u64>;
}

pub struct NodeContext<S>
where
    S: Source,
{
    pub(crate) channel_strategy: ChannelStrategy<S::Data>,
    pub(crate) watermark: u64,
}

/// All Source implementations have access to a Context object
pub struct SourceContext<'a, 'c, S, CD>
where
    S: Source,
    CD: ComponentDefinition + Sized + 'static,
{
    node_context: &'c mut NodeContext<S>,
    /// A reference to the backing ComponentDefinition
    source: &'a CD,
}

impl<'a, 'c, S, CD> SourceContext<'a, 'c, S, CD>
where
    S: Source,
    CD: ComponentDefinition + Sized + 'static,
{
    #[inline]
    pub(crate) fn new(source: &'a CD, node_context: &'c mut NodeContext<S>) -> Self {
        Self {
            node_context,
            source,
        }
    }

    #[inline]
    pub fn output(&mut self, data: S::Data, timestamp: Option<u64>) {
        let ts = match timestamp {
            Some(ts) => ts,
            None => get_system_time(),
        };

        self.update_watermark(ts);

        let elem = ArconElement::with_timestamp(data, ts);

        self.node_context
            .channel_strategy
            .add(ArconEvent::Element(elem), self.source);
    }

    #[inline(always)]
    fn update_watermark(&mut self, ts: u64) {
        if ts > self.node_context.watermark {
            self.node_context.watermark = ts;
        }
    }
    pub fn signal_end(&mut self) {
        unimplemented!();
    }
}
