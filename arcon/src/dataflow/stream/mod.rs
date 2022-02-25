pub mod bench;
pub mod build;
pub mod debug;
pub mod filter;
pub mod map;
pub mod operator;
pub mod partition;

#[allow(dead_code)]
pub mod arrow;
#[allow(dead_code)]
pub mod keyed;

use crate::{
    application::{assembled::Runtime, Application},
    data::ArconType,
    dataflow::{
        constructor::*,
        dfg::{DFGNodeKind, OperatorId},
    },
};
use std::rc::Rc;

pub use bench::BenchExt;
pub use build::BuildExt;
pub use debug::DebugExt;
pub use filter::FilterExt;
pub use map::MapExt;
pub use operator::OperatorExt;
pub use partition::PartitionExt;

pub use keyed::KeyedStream;
pub use super::builder::KeyBuilder;

/// Represents a possibly infinite stream of records
pub struct Stream<T: ArconType> {
    // ID of the node which outputs this stream.
    prev_dfg_id: OperatorId,
    ctx: Context,
    key_builder: Option<KeyBuilder<T>>,
    last_node: Option<Rc<dyn TypedNodeFactory<T>>>,
    source: Option<Rc<dyn TypedSourceFactory<T>>>,
}

impl<T: ArconType> Stream<T> {
    /// Move the optional last_node/source-Factory into the DFG struct, will no longer be mutable after this.
    fn move_last_node(&mut self) {
        if let Some(node) = self.last_node.take() {
            let prev_dfg_node = self.ctx.app.dfg.get_mut(&self.prev_dfg_id);
            assert!(matches!(&prev_dfg_node.kind, DFGNodeKind::Placeholder)); // Make sure nothing bad has happened
            prev_dfg_node.kind = DFGNodeKind::Node(node.untype());
        } else if let Some(source) = self.source.take() {
            let prev_dfg_node = self.ctx.app.dfg.get_mut(&self.prev_dfg_id);
            assert!(matches!(&prev_dfg_node.kind, DFGNodeKind::Placeholder)); // Make sure nothing bad has happened
            prev_dfg_node.kind = DFGNodeKind::Source(source.untype());
        }
    }
    fn build_runtime(&mut self) -> Runtime {
        Runtime::new(self.ctx.app.arcon_conf(), &self.ctx.app.arcon_logger)
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
    pub(crate) app: Application,
    pub(crate) console_output: bool,
}

impl Context {
    pub fn new(app: Application) -> Self {
        Self {
            app,
            console_output: false,
        }
    }
}
