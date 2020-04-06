use arcon::prelude::*;
use crate::nexmark::config::NEXMarkConfig;

#[derive(ComponentDefinition)]
#[allow(dead_code)]
pub struct NEXMarkSource<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    ctx: ComponentContext<Self>,
    source_ctx: SourceContext<IN, OUT>,
    nexmark_config: NEXMarkConfig,
}

impl<IN, OUT> NEXMarkSource<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(nexmark_config: NEXMarkConfig, source_ctx: SourceContext<IN, OUT>) -> Self {
        NEXMarkSource {
            ctx: ComponentContext::new(),
            source_ctx,
            nexmark_config,
        }
    }

    pub fn process(&mut self) {
        //let mut events_so_far: u32 = 0;
        //use rand::{rngs::SmallRng, SeedableRng};
        // TODO: add logic that creates events and sends it off using self.source_ctx
    }
}

impl<IN, OUT> Provide<ControlPort> for NEXMarkSource<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {}
    }
}

impl<IN, OUT> Actor for NEXMarkSource<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    type Message = ();
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}
