use crate::error::*;
use crate::prelude::{DeserializeOwned, Serialize};
use crate::streaming::Channel;
use kompact::{ActorPath, ActorRef, ComponentDefinition, Port, Require};
use messages::protobuf::*;
use std::fmt::Debug;
use std::hash::Hash;

pub mod broadcast;
pub mod forward;
pub mod hash;

/// `Partitioner` is used to output events to one or more channels
///
/// A: The Event to be sent
/// B: Which Port that is used for Partitioner
/// C: Source Component required for the tell method
pub trait Partitioner<A, B, C>: Send + Sync
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(
        &mut self,
        event: B::Request,
        source: *const C,
        key: Option<u64>,
    ) -> ArconResult<()>;
    fn add_channel(&mut self, channel: Channel<A, B, C>);
    fn remove_channel(&mut self, channel: Channel<A, B, C>);
}

/// `channel_output` takes an event and sends it to another
/// component. Either locally through a Port or by ActorRef, or remote (ActorPath)
fn channel_output<A, B, C>(
    channel: &Channel<A, B, C>,
    event: A,
    source: *const C,
    key: Option<u64>,
) -> ArconResult<()>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    // pointer in order to escape the double borrow issue
    // TODO: find better way...
    let source = unsafe { &(*source) };
    match &channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(Box::new(event), source);
        }
        Channel::Remote(actor_path) => {
            let serialised_event: Vec<u8> = bincode::serialize(&event).map_err(|e| {
                arcon_err_kind!("Failed to serialise event with err {}", e.to_string())
            })?;

            // TODO: Handle Timestamps...
            if let Some(key) = key {
                let keyed_msg = create_keyed_element(serialised_event, 1, key);
                actor_path.tell(keyed_msg, source);
            } else {
                let element_msg = create_element(serialised_event, 1);
                actor_path.tell(element_msg, source);
            }
        }
        Channel::Port(port_ref) => {
            (*port_ref.0).borrow_mut().trigger(event);
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::prelude::Serialize;
    use crate::streaming::{ChannelPort, RequirePortRef};
    use kompact::default_components::*;
    use kompact::*;
    use rand::Rng;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    #[derive(ComponentDefinition)]
    #[allow(dead_code)]
    pub struct TestComp {
        ctx: ComponentContext<TestComp>,
        pub prov_port: ProvidedPort<ChannelPort<Input>, TestComp>,
        pub counter: u64,
    }

    impl TestComp {
        pub fn new() -> TestComp {
            TestComp {
                ctx: ComponentContext::new(),
                prov_port: ProvidedPort::new(),
                counter: 0,
            }
        }
    }
    impl Provide<ControlPort> for TestComp {
        fn handle(&mut self, event: ControlEvent) -> () {}
    }

    impl Actor for TestComp {
        fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
            self.counter += 1;
        }
        fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
            self.counter += 1;
        }
    }
    impl Require<ChannelPort<Input>> for TestComp {
        fn handle(&mut self, event: ()) -> () {
            // ignore
        }
    }
    impl Provide<ChannelPort<Input>> for TestComp {
        fn handle(&mut self, event: Input) -> () {
            self.counter += 1;
        }
    }

    #[repr(C)]
    #[key_by(id)]
    #[derive(Clone, Debug, Copy, Serialize, Deserialize)]
    pub struct Input {
        pub id: u32,
    }
}
