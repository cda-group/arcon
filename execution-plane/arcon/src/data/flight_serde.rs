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
    use crate::data::{
        ArconElement, ArconEvent, ArconMessage, ArconType, Epoch, NodeID, Watermark,
    };
    use kompact::prelude::*;
    use prost::*;

    #[derive(prost::Message, Clone)]
    pub struct NetworkMessage {
        #[prost(message, tag = "1")]
        pub sender: Option<NodeID>,
        #[prost(message, repeated, tag = "2")]
        pub events: Vec<RawEvent>,
    }

    #[derive(prost::Message, Clone)]
    pub struct RawEvent {
        #[prost(bytes, tag = "1")]
        pub bytes: Vec<u8>,
        #[prost(enumeration = "EventType", tag = "2")]
        pub event_type: i32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
    pub enum EventType {
        Element = 0,
        Watermark = 1,
        Epoch = 2,
        Death = 3,
    }

    #[derive(Clone, Debug)]
    pub struct ReliableSerde<A: ArconType>(pub ArconMessage<A>);

    impl<A: ArconType> ReliableSerde<A> {
        const SID: kompact::prelude::SerId = 25;
    }

    impl<A: ArconType> Deserialiser<ArconMessage<A>> for ReliableSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut dyn Buf) -> Result<ArconMessage<A>, SerError> {
            let network_msg = NetworkMessage::decode(buf.bytes()).map_err(|_| {
                SerError::InvalidData("Failed to decode NetworkMessage".to_string())
            })?;
            let mut events: Vec<ArconEvent<A>> = Vec::with_capacity(network_msg.events.len());
            for raw_event in network_msg.events {
                match raw_event.event_type {
                    x if x == EventType::Element as i32 => {
                        let elem: ArconElement<A> =
                            ArconElement::decode(raw_event.bytes.into_buf()).map_err(|_| {
                                SerError::InvalidData("Failed to decode Element".to_string())
                            })?;
                        events.push(ArconEvent::Element(elem));
                    }
                    x if x == EventType::Watermark as i32 => {
                        let watermark: Watermark = Watermark::decode(raw_event.bytes.into_buf())
                            .map_err(|_| {
                                SerError::InvalidData("Failed to decode Watermark".to_string())
                            })?;
                        events.push(ArconEvent::Watermark(watermark));
                    }
                    x if x == EventType::Epoch as i32 => {
                        let epoch: Epoch =
                            Epoch::decode(raw_event.bytes.into_buf()).map_err(|_| {
                                SerError::InvalidData("Failed to decode Epoch".to_string())
                            })?;
                        events.push(ArconEvent::Epoch(epoch));
                    }
                    x if x == EventType::Death as i32 => {
                        let death: String =
                            String::decode(raw_event.bytes.into_buf()).map_err(|_| {
                                SerError::InvalidData("Failed to decode Death".to_string())
                            })?;
                        events.push(ArconEvent::Death(death));
                    }
                    _ => {
                        panic!("Matched unknown EventType");
                    }
                }
            }
            if let Some(sender) = network_msg.sender {
                Ok(ArconMessage { events, sender })
            } else {
                return Err(SerError::InvalidData("Failed to decode NodeID".to_string()));
            }
        }
    }
    impl<A: ArconType> Serialisable for ReliableSerde<A> {
        fn ser_id(&self) -> u64 {
            Self::SID
        }
        fn size_hint(&self) -> Option<usize> {
            let mut total_len = 0;
            for event in &self.0.events {
                let event_len = match &event {
                    ArconEvent::Element(e) => e.data.as_ref().unwrap().encoded_len(),
                    ArconEvent::Watermark(w) => w.encoded_len(),
                    ArconEvent::Epoch(epoch) => epoch.encoded_len(),
                    ArconEvent::Death(death) => death.encoded_len(),
                };
                total_len += event_len + 6; // RawEvent
            }
            total_len += 4; // size of NetworkMessage
            Some(total_len)
        }

        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            let mut raw_events: Vec<RawEvent> = Vec::with_capacity(self.0.events.len());

            for event in &self.0.events {
                match event {
                    ArconEvent::Element(elem) => {
                        let mut bytes = Vec::with_capacity(elem.encoded_len());
                        elem.encode(&mut bytes).map_err(|_| {
                            SerError::InvalidData("Failed to encode element".to_string())
                        })?;
                        raw_events.push(RawEvent {
                            bytes,
                            event_type: EventType::Element as i32,
                        });
                    }
                    ArconEvent::Watermark(watermark) => {
                        let mut bytes = Vec::with_capacity(watermark.encoded_len());
                        watermark.encode(&mut bytes).map_err(|_| {
                            SerError::InvalidData("Failed to encode watermark".to_string())
                        })?;
                        raw_events.push(RawEvent {
                            bytes,
                            event_type: EventType::Watermark as i32,
                        });
                    }
                    ArconEvent::Epoch(epoch) => {
                        let mut bytes = Vec::with_capacity(epoch.encoded_len());
                        epoch.encode(&mut bytes).map_err(|_| {
                            SerError::InvalidData("Failed to encode epoch".to_string())
                        })?;
                        raw_events.push(RawEvent {
                            bytes,
                            event_type: EventType::Epoch as i32,
                        });
                    }
                    ArconEvent::Death(death) => {
                        let mut bytes = Vec::with_capacity(death.encoded_len());
                        death.encode(&mut bytes).map_err(|_| {
                            SerError::InvalidData("Failed to encode death".to_string())
                        })?;
                        raw_events.push(RawEvent {
                            bytes,
                            event_type: EventType::Death as i32,
                        });
                    }
                }
            }

            let network_msg = NetworkMessage {
                sender: Some(self.0.sender),
                events: raw_events,
            };

            unsafe {
                network_msg.encode(&mut buf.bytes_mut()).map_err(|_| {
                    SerError::InvalidData("Failed to encode network message".to_string())
                })?;
                buf.advance_mut(network_msg.encoded_len());
            }

            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }
}

/// Module containing the [kompact] serialiser/deserialiser implementation for [FlightSerde::Unsafe]
pub mod unsafe_remote {
    use crate::data::{ArconMessage, ArconType};
    use kompact::prelude::*;

    #[derive(Clone, Debug)]
    pub struct UnsafeSerde<A: ArconType>(pub ArconMessage<A>);

    impl<A: ArconType> UnsafeSerde<A> {
        const SID: kompact::prelude::SerId = 26;
    }

    impl<A: ArconType> Deserialiser<ArconMessage<A>> for UnsafeSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut dyn Buf) -> Result<ArconMessage<A>, SerError> {
            // TODO: improve
            // But might need a BufMut rather than a Buf...
            let bytes = buf.bytes();
            let mut tmp_buf: Vec<u8> = Vec::with_capacity(bytes.len());
            tmp_buf.put_slice(&bytes);
            if let Some((msg, _)) = unsafe { abomonation::decode::<ArconMessage<A>>(&mut tmp_buf) }
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
                abomonation::encode(&self.0, &mut buf.bytes_mut()).map_err(|_| {
                    SerError::InvalidData("Failed to encode flight data".to_string())
                })?;
                buf.advance_mut(buf.remaining_mut());
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
    use crate::data::test::ArconDataTest;
    use crate::prelude::*;
    use kompact::prelude::*;

    fn get_systems() -> (KompactSystem, KompactSystem) {
        let system = || {
            let mut cfg = KompactConfig::new();
            cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
            cfg.build().expect("KompactSystem")
        };
        (system(), system())
    }

    #[test]
    fn unsafe_serde_test() {
        let (local, remote) = get_systems();
        let timeout = std::time::Duration::from_millis(150);
        let comp = remote.create(move || DebugNode::<ArconDataTest>::new());
        remote
            .start_notify(&comp)
            .wait_timeout(timeout)
            .expect("comp never started");

        let comp_id = format!("comp");
        let _ = remote.register_by_alias(&comp, comp_id.clone());
        let remote_path = ActorPath::Named(NamedPath::with_system(
            remote.system_path(),
            vec![comp_id.into()],
        ));

        let dispatcher_ref = local.dispatcher_ref();
        let channel = Channel::Remote(remote_path, FlightSerde::Unsafe, dispatcher_ref.into());
        let mut channel_strategy: ChannelStrategy<ArconDataTest> =
            ChannelStrategy::Forward(Forward::new(channel, 1.into()));

        let items = vec![1, 2, 3, 4, 5, 6, 7];
        let data = ArconDataTest {
            id: 1,
            items: items.clone(),
        };
        let element = ArconElement::new(data);
        channel_strategy.add(ArconEvent::Element(element));
        channel_strategy.flush();
        std::thread::sleep(timeout);
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, 1);
        }
        let _ = local.shutdown();
        let _ = remote.shutdown();
    }
}
