// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// An Enum holding possible serialisation/deserialisation options for in-flight data
#[derive(Clone)]
pub enum ArconSerde {
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

impl Default for ArconSerde {
    fn default() -> Self {
        ArconSerde::Reliable
    }
}

/// Module containing the [kompact] serialiser/deserialiser implementation for [ArconSerde::Reliable]
pub mod reliable_remote {
    use crate::data::{ArconEvent, ArconMessage, ArconType, Epoch, NodeID, Watermark};
    use kompact::prelude::*;
    use prost::*;

    #[derive(prost::Message, Clone)]
    pub struct ArconNetworkMessage {
        #[prost(message, tag = "1")]
        pub sender: Option<NodeID>,
        #[prost(bytes, tag = "2")]
        pub data: Vec<u8>,
        #[prost(message, tag = "3")]
        pub timestamp: Option<u64>,
        #[prost(enumeration = "Payload", tag = "4")]
        pub payload: i32,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
    pub enum Payload {
        Element = 0,
        Watermark = 1,
        Epoch = 2,
    }

    #[derive(Clone, Debug)]
    pub struct ReliableSerde<A: ArconType>(pub ArconMessage<A>);

    impl<A: ArconType> ReliableSerde<A> {
        const SID: kompact::prelude::SerId = 25;
    }

    impl<A: ArconType> Deserialiser<ArconMessage<A>> for ReliableSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut dyn Buf) -> Result<ArconMessage<A>, SerError> {
            let mut res = ArconNetworkMessage::decode(buf.bytes()).map_err(|_| {
                SerError::InvalidData("Failed to decode ArconNetworkMessage".to_string())
            })?;

            match res.payload {
                _ if res.payload == Payload::Element as i32 => {
                    let element: A = A::decode_storage(&mut res.data).map_err(|_| {
                        SerError::InvalidData("Failed to decode ArconType".to_string())
                    })?;
                    Ok(ArconMessage::<A>::element(
                        element,
                        res.timestamp,
                        res.sender.unwrap(),
                    ))
                }
                _ if res.payload == Payload::Watermark as i32 => {
                    let mut buf = res.data.into_buf();
                    let wm = Watermark::decode(&mut buf).map_err(|_| {
                        SerError::InvalidData("Failed to decode Watermark".to_string())
                    })?;
                    Ok(ArconMessage::<A> {
                        // TODO: fix
                        events: vec![ArconEvent::<A>::Watermark(wm)],
                        sender: res.sender.unwrap(),
                    })
                }
                _ if res.payload == Payload::Epoch as i32 => {
                    let mut buf = res.data.into_buf();
                    let epoch = Epoch::decode(&mut buf)
                        .map_err(|_| SerError::InvalidData("Failed to decode Epoch".to_string()))?;
                    Ok(ArconMessage::<A> {
                        events: vec![ArconEvent::<A>::Epoch(epoch)],
                        sender: res.sender.unwrap(),
                    })
                }
                _ => {
                    panic!("Something went wrong while deserialising payload");
                }
            }
        }
    }
    impl<A: ArconType> Serialisable for ReliableSerde<A> {
        fn ser_id(&self) -> u64 {
            Self::SID
        }
        fn size_hint(&self) -> Option<usize> {
            /*
            let event_len = match &self.0.event {
                ArconEvent::Element(e) => e.data.encoded_len(),
                ArconEvent::Watermark(w) => w.encoded_len(),
                ArconEvent::Epoch(epoch) => epoch.encoded_len(),
            };

            let size = event_len + 2 + 2 + 2; //  sender + timestamp + payload type
            Some(size)
            */
            // TODO: FIX
            None
        }

        fn serialise(&self, _: &mut dyn BufMut) -> Result<(), SerError> {
            panic!("FIX");
            /*
            // TODO: Quite inefficient as of now. fix..
            match &self.0.event {
                ArconEvent::Element(elem) => {
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(self.0.sender),
                        data: elem.data.encode_storage().unwrap(),
                        timestamp: elem.timestamp,
                        payload: Payload::Element as i32,
                    };
                    let mut data_buf = Vec::with_capacity(remote_msg.encoded_len());
                    remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
                ArconEvent::Watermark(wm) => {
                    let mut wm_buf = Vec::with_capacity(wm.encoded_len());
                    wm.encode(&mut wm_buf).map_err(|_| {
                        SerError::InvalidData(
                            "Failed to encode Watermark for remote msg".to_string(),
                        )
                    })?;
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(self.0.sender),
                        data: wm_buf,
                        timestamp: Some(wm.timestamp),
                        payload: Payload::Watermark as i32,
                    };
                    let mut data_buf = Vec::with_capacity(remote_msg.encoded_len());
                    remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
                ArconEvent::Epoch(e) => {
                    let mut epoch_buf = Vec::with_capacity(e.encoded_len());
                    e.encode(&mut epoch_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode Epoch for remote msg".to_string())
                    })?;
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(self.0.sender),
                        data: epoch_buf,
                        timestamp: None,
                        payload: Payload::Epoch as i32,
                    };
                    let mut data_buf = Vec::with_capacity(remote_msg.encoded_len());
                    remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
            }
            */
            //Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }
}

/// Module containing the [kompact] serialiser/deserialiser implementation for [ArconSerde::Unsafe]
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

        let channel = Channel::Remote((remote_path, ArconSerde::Unsafe));
        let mut channel_strategy: ChannelStrategy<ArconDataTest> =
            ChannelStrategy::Forward(Forward::new(channel, 1.into()));

        let items = vec![1, 2, 3, 4, 5, 6, 7];
        let data = ArconDataTest {
            id: 1,
            items: items.clone(),
        };
        let element = ArconElement::new(data);
        channel_strategy.add_and_flush(ArconEvent::Element(element), &local);
        std::thread::sleep(timeout);
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, 1);
        }
        let _ = local.shutdown();
        let _ = remote.shutdown();
    }
}
