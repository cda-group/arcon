use crate::data::ArconType;

#[derive(Clone)]
pub enum ArconSerde<A: ArconType> {
    Unsafe(unsafe_remote::UnsafeSerde<A>),
    Reliable(reliable_remote::ReliableSerde<A>),
}

impl<A: ArconType> Default for ArconSerde<A> {
    fn default() -> Self {
        ArconSerde::Reliable(reliable_remote::ReliableSerde::<A>::new())
    }
}

pub(crate) mod reliable_remote {
    use crate::data::{ArconEvent, ArconMessage, ArconType, Epoch, NodeID, Watermark};
    use bytes::IntoBuf;
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

    #[derive(Clone)]
    pub struct ReliableSerde<A: ArconType> {
        pub marker: std::marker::PhantomData<A>,
    }

    impl<A: ArconType> ReliableSerde<A> {
        const SID: kompact::prelude::SerId = 25;
        pub fn new() -> Self {
            ReliableSerde {
                marker: std::marker::PhantomData,
            }
        }
    }
    impl<A: ArconType> Deserialiser<ArconMessage<A>> for ReliableSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut Buf) -> Result<ArconMessage<A>, SerError> {
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
                        event: ArconEvent::<A>::Watermark(wm),
                        sender: res.sender.unwrap(),
                    })
                }
                _ if res.payload == Payload::Epoch as i32 => {
                    let mut buf = res.data.into_buf();
                    let epoch = Epoch::decode(&mut buf)
                        .map_err(|_| SerError::InvalidData("Failed to decode Epoch".to_string()))?;
                    Ok(ArconMessage::<A> {
                        event: ArconEvent::<A>::Epoch(epoch),
                        sender: res.sender.unwrap(),
                    })
                }
                _ => {
                    panic!("Something went wrong while deserialising payload");
                }
            }
        }
    }
    impl<A: ArconType> Serialiser<ArconMessage<A>> for ReliableSerde<A> {
        fn ser_id(&self) -> u64 {
            Self::SID
        }
        fn size_hint(&self) -> Option<usize> {
            // This needs to be fixed..
            // Not possible to know the exact size without having the object..
            Some(std::mem::size_of::<ArconNetworkMessage>())
        }
        fn serialise(&self, msg: &ArconMessage<A>, buf: &mut dyn BufMut) -> Result<(), SerError> {
            // TODO: Quite inefficient as of now. fix..
            match &msg.event {
                ArconEvent::Element(elem) => {
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(msg.sender),
                        data: elem.data.encode_storage().unwrap(),
                        timestamp: elem.timestamp,
                        payload: Payload::Element as i32,
                    };
                    let mut data_buf = Vec::new();
                    let _ = remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
                ArconEvent::Watermark(wm) => {
                    let mut wm_buf = Vec::new();
                    let _ = wm.encode(&mut wm_buf).map_err(|_| {
                        SerError::InvalidData(
                            "Failed to encode Watermark for remote msg".to_string(),
                        )
                    })?;
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(msg.sender),
                        data: wm_buf,
                        timestamp: Some(wm.timestamp),
                        payload: Payload::Watermark as i32,
                    };
                    let mut data_buf = Vec::new();
                    let _ = remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
                ArconEvent::Epoch(e) => {
                    let mut epoch_buf = Vec::new();
                    let _ = e.encode(&mut epoch_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode Epoch for remote msg".to_string())
                    })?;
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(msg.sender),
                        data: epoch_buf,
                        timestamp: None,
                        payload: Payload::Epoch as i32,
                    };
                    let mut data_buf = Vec::new();
                    let _ = remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
            }
            Ok(())
        }
    }
}

pub(crate) mod unsafe_remote {
    use crate::data::{ArconMessage, ArconType};
    use kompact::prelude::*;

    #[derive(Clone)]
    pub struct UnsafeSerde<A: ArconType> {
        marker: std::marker::PhantomData<A>,
    }

    impl<A: ArconType> UnsafeSerde<A> {
        const SID: kompact::prelude::SerId = 26;
        pub fn new() -> Self {
            UnsafeSerde {
                marker: std::marker::PhantomData,
            }
        }
    }

    impl<A: ArconType> Deserialiser<ArconMessage<A>> for UnsafeSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut Buf) -> Result<ArconMessage<A>, SerError> {
            // TODO: improve
            let mut bytes = buf.bytes();
            let mut tmp_buf: Vec<u8> = Vec::with_capacity(bytes.len());
            tmp_buf.put_slice(&mut bytes);
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

    impl<A: ArconType> Serialiser<ArconMessage<A>> for UnsafeSerde<A> {
        fn ser_id(&self) -> u64 {
            Self::SID
        }
        fn size_hint(&self) -> Option<usize> {
            // NOTE: This will not work in this case where we have heap allocated fields
            Some(std::mem::size_of::<ArconMessage<A>>())
        }

        // TODO: Needs to ensure there is enough space allocated in the buf for the
        //       object size. i.e. abomonation::measure(msg)
        fn serialise(&self, msg: &ArconMessage<A>, _buf: &mut dyn BufMut) -> Result<(), SerError> {
            let total = abomonation::measure(msg);
            let mut buf_mut = bytes::BytesMut::with_capacity(total);
            let _ = unsafe {
                abomonation::encode(msg, &mut buf_mut.bytes_mut()).map_err(|_| {
                    SerError::InvalidData("Failed to encode flight data".to_string())
                })?;
            };
            panic!("Not supported yet due to non-dynamic buf allocation");
            //Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data::test::ArconDataTest;
    use crate::prelude::*;
    use crate::streaming::sink::debug::DebugSink;
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
    #[should_panic]
    fn unsafe_serde_test() {
        let (local, remote) = get_systems();
        let timeout = std::time::Duration::from_millis(150);
        let comp = remote.create(move || DebugSink::<ArconDataTest>::new());
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

        let channel = Channel::Remote((remote_path, ArconSerde::Unsafe(UnsafeSerde::new())));
        let mut channel_strategy: Box<ChannelStrategy<ArconDataTest>> =
            Box::new(Forward::new(channel));

        let items = vec![1, 2, 3, 4, 5, 6, 7];
        let data = ArconDataTest {
            id: 1,
            items: items.clone(),
        };
        let element = ArconMessage::element(data, None, 1.into());
        let _ = channel_strategy.output(element, &local);
        std::thread::sleep(timeout);
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, 1);
        }
        let _ = local.shutdown();
        let _ = remote.shutdown();
    }
}
