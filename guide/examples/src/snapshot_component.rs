use arcon::prelude::*;

#[derive(ComponentDefinition)]
pub struct SnapshotComponent {
    ctx: ComponentContext<Self>,
}

impl SnapshotComponent {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
        }
    }
}

impl ComponentLifecycle for SnapshotComponent {
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }
}

impl Actor for SnapshotComponent {
    type Message = Snapshot;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        info!(self.ctx.log(), "I received Snapshot {:?}", msg);
        Handled::Ok
    }

    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unreachable!();
    }
}
