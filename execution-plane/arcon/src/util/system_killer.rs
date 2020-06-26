use crate::data::{ArconEvent, ArconMessage, ArconNever};
use kompact::prelude::*;

/// Utility actor that shuts down the underlying kompact system on receiving an [`ArconMessage`]
/// that contains an [`ArconEvent::Death`]. Designed to be connected _after_ a node with sink
/// operator.
#[derive(ComponentDefinition)]
pub struct SystemKiller {
    ctx: ComponentContext<Self>,
}

impl Actor for SystemKiller {
    type Message = ArconMessage<ArconNever>;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        for ev in msg.events.as_slice() {
            let ev = ev.unwrap_ref();
            match ev {
                ArconEvent::Death(s) => {
                    info!(self.log(), "Received Death event: {}", s);
                    info!(self.log(), "Shutting down the kompact system");
                    self.ctx.system().shutdown_async();
                }
                _ => trace!(self.log(), "Ignoring non-death event: {:?}", ev),
            }
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> () {
        // TODO: for now we ignore all network messages
    }
}

ignore_control!(SystemKiller);

impl SystemKiller {
    pub(crate) fn new() -> SystemKiller {
        SystemKiller {
            ctx: ComponentContext::new(),
        }
    }
}
