use kompact::KompactSystem;
use crate::data::*;
use crate::error::*;
use crate::streaming::channel::Channel;

pub mod broadcast;
pub mod forward;
pub mod key_by;
pub mod round_robin;
pub mod shuffle;

/// `ChannelStrategy` is used to output events to one or more channels
///
/// A: The Event to be sent
/// B: Source Component required for the tell method
pub trait ChannelStrategy<A>: Send + Sync
where
    A: 'static + ArconType,
{
    fn output(&mut self, event: ArconEvent<A>, source: &KompactSystem) -> ArconResult<()>;
    fn add_channel(&mut self, channel: Channel);
    fn remove_channel(&mut self, channel: Channel);
}

/// `channel_output` takes an event and sends it to another component.
/// Either locally through an ActorRef, or remote (ActorPath)
fn channel_output<A>(
    channel: &Channel,
    event: ArconEvent<A>,
    source: &KompactSystem,
) -> ArconResult<()>
where
    A: 'static + ArconType,
{
    match channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(Box::new(event), source);
        }
        Channel::Remote(actor_path) => {
            actor_path.tell(event.to_remote()?, source);
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use kompact::*;

    #[derive(ComponentDefinition)]
    #[allow(dead_code)]
    pub struct TestComp {
        ctx: ComponentContext<TestComp>,
        pub counter: u64,
    }

    impl TestComp {
        pub fn new() -> TestComp {
            TestComp {
                ctx: ComponentContext::new(),
                counter: 0,
            }
        }
    }
    impl Provide<ControlPort> for TestComp {
        fn handle(&mut self, _event: ControlEvent) -> () {}
    }

    impl Actor for TestComp {
        fn receive_local(&mut self, _sender: ActorRef, _msg: &Any) {
            self.counter += 1;
        }
        fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
            self.counter += 1;
        }
    }

    #[key_by(id)]
    #[arcon]
    pub struct Input {
        pub id: u32,
    }
}
