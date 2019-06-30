use crate::error::ErrorKind::*;
use crate::error::*;
use crate::prelude::{DeserializeOwned, Serialize};
use crate::streaming::partitioner::channel_output;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use kompact::{ComponentDefinition, Port, Require};
use messages::protobuf::*;
use std::fmt::Debug;
use std::hash::Hash;

pub struct Forward<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    out_channel: Channel<A, B, C>,
}

impl<A, B, C> Forward<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    pub fn new(out_channel: Channel<A, B, C>) -> Forward<A, B, C> {
        Forward { out_channel }
    }
}

impl<A, B, C> Partitioner<A, B, C> for Forward<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(&mut self, event: A, source: *const C, key: Option<u64>) -> crate::error::Result<()> {
        channel_output(&self.out_channel, event, source, key)
    }
    fn add_channel(&mut self, channel: Channel<A, B, C>) {
        // ignore
    }
    fn remove_channel(&mut self, channel: Channel<A, B, C>) {
        // ignore
    }
}

unsafe impl<A, B, C> Send for Forward<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

unsafe impl<A, B, C> Sync for Forward<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::Serialize;
    use crate::streaming::partitioner::tests::*;
    use crate::streaming::{ChannelPort, RequirePortRef};
    use kompact::default_components::*;
    use kompact::*;
    use rand::Rng;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    #[test]
    fn forward_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u32 = 8;
        let total_msgs = 10;
        let comp = system.create_and_start(move || TestComp::new());
        let target_port = comp.on_definition(|c| c.prov_port.share());

        let mut req_port: RequiredPort<ChannelPort<Input>, TestComp> = RequiredPort::new();
        let _ = req_port.connect(target_port);

        let ref_port = RequirePortRef(Rc::new(RefCell::new(req_port)));
        let comp_channel: Channel<Input, ChannelPort<Input>, TestComp> = Channel::Port(ref_port);

        let mut partitioner: Box<Partitioner<Input, ChannelPort<Input>, TestComp>> =
            Box::new(Forward::new(comp_channel));

        for i in 0..total_msgs {
            // NOTE: second parameter is a fake channel...
            let input = Input { id: 1 };
            let _ = partitioner.output(input, &*comp.definition().lock().unwrap(), None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut comp_inspect = &comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.counter, total_msgs);
        system.shutdown();
    }
}
