use arcon::{ignore_persist, ignore_timeout, prelude::*};
use std::sync::Arc;

pub struct MyOperator;

impl Operator for MyOperator {
    type IN = u64;
    type OUT = u64;
    type TimerState = ArconNever;
    type OperatorState = ();

    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        mut ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()> {
        ctx.output(ArconElement {
            data: element.data + 10,
            timestamp: element.timestamp,
        });

        Ok(())
    }
    ignore_timeout!();
    ignore_persist!();
}

fn main() {
    let mut pipeline = Pipeline::default()
        .collection((0..100).collect::<Vec<u64>>(), |conf| {
            conf.set_arcon_time(ArconTime::Process);
        })
        .filter(|x: &u64| *x > 50)
        .operator(
            |_: Arc<Sled>| MyOperator,
            |conf| {
                conf.set_state_id("MyOperator");
            },
        )
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
