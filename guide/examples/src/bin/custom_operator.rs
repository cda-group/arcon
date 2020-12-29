use arcon::{ignore_persist, ignore_timeout, prelude::*};
use std::sync::Arc;

#[cfg_attr(feature = "arcon_serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "unsafe_flight", derive(abomonation_derive::Abomonation))]
// ANCHOR: data
#[derive(Arcon, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct CustomEvent {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
// ANCHOR_END: data

// ANCHOR: operator
pub struct MyOperator;

impl Operator for MyOperator {
    type IN = u64;
    type OUT = CustomEvent;
    type TimerState = ArconNever;
    type OperatorState = ();

    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        mut ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()> {
        let custom_event = CustomEvent { id: element.data };

        ctx.output(ArconElement {
            data: custom_event,
            timestamp: element.timestamp,
        });

        Ok(())
    }
    ignore_timeout!();
    ignore_persist!();
}
// ANCHOR_END: operator

// ANCHOR: timer_operator
pub struct TimerOperator;

impl Operator for TimerOperator {
    type IN = CustomEvent;
    type OUT = CustomEvent;
    type TimerState = u64;
    type OperatorState = ();

    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        mut ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()> {
        let current_time = ctx.current_time();
        let key = element.data.get_key();
        let time = current_time + 1000;

        if let Err(err) = ctx.schedule_at(key, time, element.data.id) {
            error!(ctx.log(), "Failed to schedule timer with err {}", err);
        }

        ctx.output(element);

        Ok(())
    }

    fn handle_timeout(
        &mut self,
        timeout: Self::TimerState,
        ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()> {
        info!(ctx.log(), "Got a timer timeout for {:?}", timeout);
        Ok(())
    }

    ignore_persist!();
}
// ANCHOR_END: timer_operator

fn main() {
    // ANCHOR: pipeline
    let mut pipeline = Pipeline::default()
        .collection((0..10000000).collect::<Vec<u64>>(), |conf| {
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .operator(
            |_: Arc<Sled>| MyOperator,
            |conf| {
                conf.set_state_id("MyOperator");
            },
        )
        .operator(
            |_: Arc<Sled>| TimerOperator,
            |conf| {
                conf.set_state_id("TimerOperator");
            },
        )
        .build();
    // ANCHOR_END: pipeline

    pipeline.start();
    pipeline.await_termination();
}
