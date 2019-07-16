use crate::data::{ArconElement, ArconType};
use crate::error::*;
use crate::messages::protobuf::*;
use crate::streaming::Channel;
use kompact::{ComponentDefinition, Port, Require};

pub mod broadcast;
pub mod forward;
pub mod hash;
pub mod round_robin;
pub mod shuffle;

/// `Partitioner` is used to output events to one or more channels
///
/// A: The Event to be sent
/// B: Which Port that is used for Partitioner
/// C: Source Component required for the tell method
pub trait Partitioner<A, B, C>: Send + Sync
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(
        &mut self,
        element: B::Request,
        source: *const C,
        key: Option<u64>,
    ) -> ArconResult<()>;
    fn add_channel(&mut self, channel: Channel<A, B, C>);
    fn remove_channel(&mut self, channel: Channel<A, B, C>);
}

/// `channel_output` takes an event and sends it to another component.
/// Either locally through a Port or by ActorRef, or remote (ActorPath)
fn channel_output<A, B, C>(
    channel: &Channel<A, B, C>,
    element: ArconElement<A>,
    source: *const C,
    key: Option<u64>,
) -> ArconResult<()>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    // pointer in order to escape the double borrow issue
    // TODO: find better way...
    let source = unsafe { &(*source) };
    match &channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(Box::new(element), source);
        }
        Channel::Remote(actor_path) => {
            let serialised_event: Vec<u8> = bincode::serialize(&element.data).map_err(|e| {
                arcon_err_kind!("Failed to serialise event with err {}", e.to_string())
            })?;

            let timestamp = element.timestamp.unwrap_or(0);
            if let Some(key) = key {
                let keyed_msg = create_keyed_element(serialised_event, timestamp, key);
                actor_path.tell(keyed_msg, source);
            } else {
                let element_msg = create_element(serialised_event, timestamp);
                actor_path.tell(element_msg, source);
            }
        }
        Channel::Port(port_ref) => {
            let required_port = (*port_ref.0).get();
            unsafe {
                (*required_port).trigger(element);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::streaming::ChannelPort;
    use kompact::*;

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
    impl Require<ChannelPort<Input>> for TestComp {
        fn handle(&mut self, _event: ()) -> () {
            // ignore
        }
    }
    impl Provide<ChannelPort<Input>> for TestComp {
        fn handle(&mut self, _event: ArconElement<Input>) -> () {
            self.counter += 1;
        }
    }

    #[key_by(id)]
    #[arcon]
    pub struct Input {
        pub id: u32,
    }
}
