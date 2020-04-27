// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// An Enum holding possible serialisation/deserialisation options for in-flight data
#[derive(Clone)]
pub enum FlightSerde {
    /// Unsafe, but highly performant option
    ///
    /// For Unsafe to be a feasible choice, the remote machine
    /// must have the same underlying architecture and data layout. An example of such an execution
    /// is where two processes on the same machine transfer serialised data over loopback.
    Unsafe,
    /// A rather slower option using Protobuf to serialise and deserialise.
    ///
    /// Reliable is the default serde option.
    Reliable,
}

impl Default for FlightSerde {
    fn default() -> Self {
        FlightSerde::Reliable
    }
}

/// Module containing the [kompact] serialiser/deserialiser implementation for [FlightSerde::Reliable]
pub mod reliable_remote {
    use crate::data::{ArconType, RawArconMessage};
    use kompact::prelude::*;
    use prost::*;

    #[derive(Clone, Debug)]
    pub struct ReliableSerde<A: ArconType>(pub RawArconMessage<A>);

    impl<A: ArconType> ReliableSerde<A> {
        const SID: kompact::prelude::SerId = 25;
    }

    impl<A: ArconType> Deserialiser<RawArconMessage<A>> for ReliableSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut dyn Buf) -> Result<RawArconMessage<A>, SerError> {
            let arcon_message = RawArconMessage::decode(buf.bytes()).map_err(|_| {
                SerError::InvalidData("Failed to decode RawArconMessage".to_string())
            })?;
            Ok(arcon_message)
        }
    }
    impl<A: ArconType> Serialisable for ReliableSerde<A> {
        fn ser_id(&self) -> u64 {
            Self::SID
        }
        fn size_hint(&self) -> Option<usize> {
            Some(self.0.encoded_len())
        }

        fn serialise(&self, mut buf: &mut dyn BufMut) -> Result<(), SerError> {
            self.0.encode(&mut buf).map_err(|_| {
                SerError::InvalidData("Failed to encode RawArconMessage".to_string())
            })?;

            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }
}

/// Module containing the [kompact] serialiser/deserialiser implementation for [FlightSerde::Unsafe]
pub mod unsafe_remote {
    use crate::data::{ArconType, BufMutWriter, RawArconMessage};
    use kompact::prelude::*;

    #[derive(Clone, Debug)]
    pub struct UnsafeSerde<A: ArconType>(pub RawArconMessage<A>);

    impl<A: ArconType> UnsafeSerde<A> {
        const SID: kompact::prelude::SerId = 26;
    }

    impl<A: ArconType> Deserialiser<RawArconMessage<A>> for UnsafeSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut dyn Buf) -> Result<RawArconMessage<A>, SerError> {
            // TODO: improve
            // But might need a BufMut rather than a Buf...
            let bytes = buf.bytes();
            let mut tmp_buf: Vec<u8> = Vec::with_capacity(bytes.len());
            tmp_buf.put_slice(&bytes);
            if let Some((msg, _)) =
                unsafe { abomonation::decode::<RawArconMessage<A>>(&mut tmp_buf) }
            {
                Ok(msg.clone())
            } else {
                Err(SerError::InvalidData(
                    "Failed to decode flight data".to_string(),
                ))
            }
        }
    }

    impl<A: ArconType> Serialisable for UnsafeSerde<A> {
        fn ser_id(&self) -> u64 {
            Self::SID
        }
        fn size_hint(&self) -> Option<usize> {
            Some(abomonation::measure(&self.0))
        }

        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            unsafe {
                let mut writer = BufMutWriter::new(buf);
                abomonation::encode(&self.0, &mut writer).map_err(|_| {
                    SerError::InvalidData("Failed to encode flight data".to_string())
                })?;
            };
            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{data::test::ArconDataTest, prelude::*};
    use kompact::prelude::*;
    use std::time::Duration;
    fn get_systems() -> (KompactSystem, KompactSystem) {
        let system = || {
            let mut cfg = KompactConfig::new();
            cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
            cfg.build().expect("KompactSystem")
        };
        (system(), system())
    }

    #[test]
    fn reliable_serde_test() {
        serde_test(FlightSerde::Reliable);
    }

    #[test]
    fn unsafe_serde_test() {
        serde_test(FlightSerde::Unsafe)
    }

    fn serde_test(serde: FlightSerde) {
        let pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let (local, remote) = get_systems();
        let timeout = Duration::from_millis(150);
        let comp = remote.create(move || DebugNode::<ArconDataTest>::new());
        remote
            .start_notify(&comp)
            .wait_timeout(timeout)
            .expect("comp never started");

        let comp_id = format!("comp");
        let reg = remote.register_by_alias(&comp, comp_id.clone());
        reg.wait_expect(
            Duration::from_millis(1000),
            "Failed to register alias for DebugNode",
        );
        let remote_path = ActorPath::Named(NamedPath::with_system(remote.system_path(), vec![
            comp_id.into(),
        ]));

        let dispatcher_ref = local.dispatcher_ref();
        let channel = Channel::Remote(remote_path, serde, dispatcher_ref.into());
        let mut channel_strategy: ChannelStrategy<ArconDataTest> =
            ChannelStrategy::Forward(Forward::new(channel, 1.into(), pool_info));

        let items = vec![1, 2, 3, 4, 5, 6, 7];
        let data = ArconDataTest {
            id: 1,
            items: items.clone(),
        };
        let element = ArconElement::new(data.clone());
        channel_strategy.add(ArconEvent::Element(element.clone()));
        channel_strategy.add(ArconEvent::Element(element.clone()));
        channel_strategy.flush();
        channel_strategy.add(ArconEvent::Element(element.clone()));
        channel_strategy.add(ArconEvent::Element(element));
        channel_strategy.flush();
        std::thread::sleep(timeout);
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, 4);
            // Verify that the data is correct..
            assert_eq!(comp_inspect.data[0].data.items, items);
            assert_eq!(comp_inspect.data[1].data.items, items);
            assert_eq!(comp_inspect.data[2].data.items, items);
            assert_eq!(comp_inspect.data[3].data.items, items);
        }
        let _ = local.shutdown();
        let _ = remote.shutdown();
    }
}
