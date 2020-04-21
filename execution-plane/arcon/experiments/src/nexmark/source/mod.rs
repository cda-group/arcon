use crate::nexmark::{config::NEXMarkConfig, NEXMarkEvent};
use arcon::prelude::*;

#[derive(ComponentDefinition)]
#[allow(dead_code)]
pub struct NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    ctx: ComponentContext<Self>,
    source_ctx: SourceContext<OP>,
    nexmark_config: NEXMarkConfig,
    events_so_far: u64,
}

impl<OP> NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    pub fn new(nexmark_config: NEXMarkConfig, source_ctx: SourceContext<OP>) -> Self {
        NEXMarkSource {
            ctx: ComponentContext::new(),
            source_ctx,
            nexmark_config,
            events_so_far: 0,
        }
    }

    pub fn process(&mut self) {
        // TODO: create event generation with the stream timeout
        /*
        let timer = ::std::time::Instant::now();
        // Establish a start of the computation.
        let elapsed_ns: u32 = timer.elapsed().as_nanos() as u32;
        self.nexmark_config.base_time_ns = elapsed_ns as u32;
        let duration_ns: u64 = self.nexmark_config.stream_timeout * 1_000_000_000;
        */
        use rand::{rngs::SmallRng, FromEntropy};
        let mut rng = SmallRng::from_entropy();

        let mut wm_counter = 0;
        let mut events_so_far: u32 = 0;

        loop {
            let next_event = crate::nexmark::NEXMarkEvent::create(
                events_so_far,
                &mut rng,
                &mut self.nexmark_config,
            );

            let elem = self.source_ctx.extract_element(next_event);
            debug!(self.ctx().log(), "Created elem {:?}", elem);
            self.source_ctx.process(elem);
            events_so_far += 1;

            wm_counter += 1;

            if wm_counter == self.source_ctx.watermark_interval {
                self.source_ctx.generate_watermark();
                wm_counter = 0;
            }
        }
    }
}

impl<OP> Provide<ControlPort> for NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            self.process();
        }
    }
}

impl<OP> Actor for NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    type Message = ();
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}
