use crate::data::{ArconEvent, ArconType};
use crate::prelude::KompactSystem;
use crate::prelude::*;
use crate::streaming::channel::strategy::channel_output;
use fnv::FnvHasher;
use std::default::Default;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};

type DefaultHashBuilder = BuildHasherDefault<FnvHasher>;

/// A hash based partitioner
///
/// `KeyBy` may be constructed with
/// either a custom hasher or the default `FnvHasher`
#[derive(Default)]
pub struct KeyBy<A, H = DefaultHashBuilder>
where
    A: 'static + ArconType + Hash,
{
    builder: H,
    parallelism: u32,
    nodes: Vec<Channel<A>>,
}

impl<A> KeyBy<A>
where
    A: 'static + ArconType + Hash,
{
    pub fn new(parallelism: u32, channels: Vec<Channel<A>>) -> KeyBy<A> {
        assert_eq!(channels.len(), parallelism as usize);
        KeyBy {
            builder: Default::default(),
            parallelism,
            nodes: channels,
        }
    }

    pub fn with_hasher<B>(
        parallelism: u32,
        channels: Vec<Channel<A>>,
    ) -> KeyBy<A, BuildHasherDefault<B>>
    where
        B: Hasher + Default,
    {
        assert_eq!(channels.len(), parallelism as usize);
        KeyBy {
            builder: BuildHasherDefault::<B>::default(),
            parallelism,
            nodes: channels,
        }
    }
}

impl<A, B> ChannelStrategy<A> for KeyBy<A, B>
where
    A: 'static + ArconType + Hash,
    B: BuildHasher + Send + Sync,
{
    fn output(&mut self, message: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()> {
        match &message.event {
            ArconEvent::Element(element) => {
                let mut h = self.builder.build_hasher();
                element.data.hash(&mut h);
                let hash = h.finish() as u32;
                let index = (hash % self.parallelism) as usize;
                if let Some(channel) = self.nodes.get(index) {
                    let _ = channel_output(channel, message, source)?;
                } else {
                    panic!("Something went wrong while finding channel!..");
                }
            }
            _ => {
                for channel in self.nodes.iter() {
                    let _ = channel_output(channel, message.clone(), source)?;
                }
            }
        }
        Ok(())
    }

    fn add_channel(&mut self, _channel: Channel<A>) {
        unimplemented!();
    }
    fn remove_channel(&mut self, _channel: Channel<A>) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::tests::*;
    use kompact::prelude::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn partitioner_parallelism_8_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let parallelism: u32 = 8;
        let total_msgs = 1000;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugSink<Input>>>> = Vec::new();

        for _i in 0..parallelism {
            let comp = system.create_and_start(move || DebugSink::<Input>::new());
            let actor_ref: ActorRef<ArconMessage<Input>> = comp.actor_ref();
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: Box<ChannelStrategy<Input>> =
            Box::new(KeyBy::new(parallelism, channels));

        let mut rng = rand::thread_rng();

        let mut inputs: Vec<ArconMessage<Input>> = Vec::new();
        for _i in 0..total_msgs {
            let input = Input {
                id: rng.gen_range(0, 100),
            };
            inputs.push(ArconMessage::element(input, None, 1.into()));
        }

        for input in inputs {
            // Just assume it is all sent from same comp
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.data.len() > 0);
        }
        let _ = system.shutdown();
    }
}
