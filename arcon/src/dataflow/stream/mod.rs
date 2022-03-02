pub mod filter;
pub mod map;
pub mod operator;
pub mod partition;

#[allow(dead_code)]
pub mod arrow;
#[allow(dead_code)]
pub mod keyed;

use crate::{
    data::ArconType,
    dataflow::{
        constructor::*,
        dfg::{ChannelKind, DFGNodeKind, OperatorId, DFG},
    },
};
use std::rc::Rc;

pub use filter::FilterExt;
pub use map::MapExt;
pub use operator::OperatorExt;
pub use partition::PartitionExt;

pub use super::builder::KeyBuilder;
pub use keyed::KeyedStream;

/// Represents a possibly infinite stream of records
pub struct Stream<T: ArconType> {
    // ID of the node which outputs this stream.
    prev_dfg_id: OperatorId,
    pub(crate) ctx: Context,
    key_builder: Option<KeyBuilder<T>>,
    last_node: Option<Rc<dyn TypedNodeFactory<T>>>,
    source: Option<Rc<dyn TypedSourceFactory<T>>>,
}

impl<T: ArconType> Stream<T> {
    /// Move the optional last_node/source-Factory into the DFG struct, will no longer be mutable after this.
    pub(crate) fn move_last_node(&mut self) {
        if let Some(node) = self.last_node.take() {
            let prev_dfg_node = self.ctx.dfg.get_mut(&self.prev_dfg_id);
            assert!(matches!(&prev_dfg_node.kind, DFGNodeKind::Placeholder)); // Make sure nothing bad has happened
            prev_dfg_node.kind = DFGNodeKind::Node(node.untype());
        } else if let Some(source) = self.source.take() {
            let prev_dfg_node = self.ctx.dfg.get_mut(&self.prev_dfg_id);
            assert!(matches!(&prev_dfg_node.kind, DFGNodeKind::Placeholder)); // Make sure nothing bad has happened
            prev_dfg_node.kind = DFGNodeKind::Source(source.untype());
        }
    }
    pub(crate) fn set_channel_kind(&mut self, channel_kind: ChannelKind) {
        if let Some(ref mut node_factory) = self.last_node {
            Rc::get_mut(node_factory)
                .unwrap()
                .set_channel_kind(channel_kind);
        } else if let Some(ref mut source_factory) = self.source {
            Rc::get_mut(source_factory)
                .unwrap()
                .set_channel_kind(channel_kind);
        } else {
            panic!("Nothing to configure ChannelKind on!");
        }
    }

    pub(crate) fn new(ctx: Context, source: Rc<dyn TypedSourceFactory<T>>) -> Self {
        Self {
            prev_dfg_id: 0,
            ctx,
            last_node: None,
            key_builder: None,
            source: Some(source),
        }
    }
}

#[derive(Default)]
pub(crate) struct Context {
    pub(crate) dfg: DFG,
}
