use crate::{
    data::ArconType,
    dataflow::{
        dfg::ChannelKind,
        stream::Stream,
    }, prelude::ApplicationBuilder,
};

mod private {
    use super::*;
    pub trait Sealed {}
    impl<A: ArconType> Sealed for Stream<A> {}
}

/// Extension trait for build operations
pub trait BuilderExt: private::Sealed {
    /// Returns an [ApplicationBuilder] that can be used to create an [Application]
    fn builder(self) -> ApplicationBuilder;
    fn print(self) -> Self;
}

impl<T: ArconType> BuilderExt for Stream<T> {
    fn print(mut self) -> Self {
        self.set_channel_kind(ChannelKind::Console);
        self
    }
    fn builder(mut self) -> ApplicationBuilder {
        self.set_channel_kind(ChannelKind::Mute);
        self.move_last_node();
        ApplicationBuilder::new(self.ctx)
    }
}