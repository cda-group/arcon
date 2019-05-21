extern crate codegen;
use codegen::prelude::*;
#[derive(ComponentDefinition)]
pub struct Basic {
    ctx: ComponentContext<Basic>,
    udf: Module,
    udf_avg: u64,
    udf_executions: u64,
    id: String,
}
impl Basic {
    pub fn new(id: String, udf: Module) -> Basic {
        Basic {
            ctx: ComponentContext::new(),
            udf,
            udf_avg: 0,
            udf_executions: 0,
            id,
        }
    }
}
impl Provide<ControlPort> for Basic {
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                unimplemented!();
            }
            ControlEvent::Stop => {
                unimplemented!();
            }
            ControlEvent::Kill => {
                unimplemented!();
            }
        }
    }
}
impl Actor for Basic {
    fn receive_local(&mut self, sender: ActorRef, msg: Box<Any>) {}
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        unimplemented!();
    }
}
fn main() {}
