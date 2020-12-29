// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// An Enum holding possible serialisation/deserialisation options for in-flight data
#[derive(Clone)]
pub enum FlightSerde {
    /// A rather slower option using Protobuf to serialise and deserialise.
    ///
    /// Reliable is the default serde option.
    Reliable,
    #[cfg(feature = "unsafe_flight")]
    #[allow(dead_code)]
    /// Unsafe, but highly performant option
    ///
    /// For Unsafe to be a feasible choice, the remote machine
    /// must have the same underlying architecture and data layout. An example of such an execution
    /// is where two processes on the same machine transfer serialised data over loopback.
    Unsafe,
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

    impl<A: ArconType> Deserialiser<RawArconMessage<A>> for ReliableSerde<A> {
        const SER_ID: SerId = A::RELIABLE_SER_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<RawArconMessage<A>, SerError> {
            RawArconMessage::decode(buf.bytes()).map_err(|e| SerError::InvalidData(e.to_string()))
        }
    }

    impl<A: ArconType> Serialisable for ReliableSerde<A> {
        fn ser_id(&self) -> u64 {
            A::RELIABLE_SER_ID
        }
        fn size_hint(&self) -> Option<usize> {
            Some(self.0.encoded_len())
        }
        fn serialise(&self, mut buf: &mut dyn BufMut) -> Result<(), SerError> {
            self.0
                .encode(&mut buf)
                .map_err(|e| SerError::InvalidData(e.to_string()))?;

            Ok(())
        }
        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }
}

#[cfg(feature = "unsafe_flight")]
/// Module containing the [kompact] serialiser/deserialiser implementation for [FlightSerde::Unsafe]
pub mod unsafe_remote {
    use crate::data::{ArconType, BufMutWriter, RawArconMessage, VersionId};
    use kompact::prelude::*;

    #[derive(Clone, Debug)]
    pub struct UnsafeSerde<A: ArconType>(pub RawArconMessage<A>);

    impl<A: ArconType> Deserialiser<RawArconMessage<A>> for UnsafeSerde<A> {
        const SER_ID: SerId = A::UNSAFE_SER_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<RawArconMessage<A>, SerError> {
            let version_id = buf.get_u32();
            if version_id != A::VERSION_ID {
                let err = format!(
                    "Mismatch on ArconType version. Got {} while expecting {}",
                    version_id,
                    A::VERSION_ID
                );
                return Err(SerError::InvalidData(err));
            }

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
            A::UNSAFE_SER_ID
        }
        fn size_hint(&self) -> Option<usize> {
            let size = std::mem::size_of::<VersionId>() + abomonation::measure(&self.0);
            Some(size)
        }
        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            buf.put_u32(A::VERSION_ID);

            unsafe {
                let mut writer = BufMutWriter::new(buf);
                abomonation::encode(&self.0, &mut writer).map_err(|_| {
                    SerError::InvalidData("Failed to encode unsafe flight data".to_string())
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
    use crate::{
        data::{ArconElement, ArconEvent, ArconType},
        pipeline::Pipeline,
        stream::{
            channel::{
                strategy::{forward::Forward, ChannelStrategy},
                Channel,
            },
            node::debug::DebugNode,
        },
    };
    #[cfg(feature = "unsafe_flight")]
    use abomonation_derive::*;
    use kompact::prelude::*;
    use once_cell::sync::Lazy;
    use std::time::Duration;

    static ITEMS: Lazy<Vec<u32>> = Lazy::new(|| vec![1, 2, 3, 4, 5, 6, 7]);
    const PRICE: u32 = 10;
    const ID: u32 = 1;

    // The flight_serde pipeline will always send data of ArconDataTest
    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "unsafe_flight", derive(Abomonation))]
    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1)]
    pub struct ArconDataTest {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
        #[prost(uint32, tag = "3")]
        pub price: u32,
    }

    // Down below are different structs the unit tests may attempt to deserialise into

    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "unsafe_flight", derive(Abomonation))]
    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 2)]
    pub struct UpdatedVer {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
        #[prost(uint32, tag = "3")]
        pub price: u32,
    }

    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "unsafe_flight", derive(Abomonation))]
    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(unsafe_ser_id = 204, reliable_ser_id = 205, version = 2)]
    pub struct UpdatedSerId {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
        #[prost(uint32, tag = "3")]
        pub price: u32,
    }

    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "unsafe_flight", derive(Abomonation))]
    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1)]
    pub struct RemovedField {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
    }

    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "unsafe_flight", derive(Abomonation))]
    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1)]
    pub struct AddedField {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
        #[prost(uint32, tag = "3")]
        pub price: u32,
        #[prost(bytes, tag = "4")]
        pub bytes: Vec<u8>,
    }

    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "unsafe_flight", derive(Abomonation))]
    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1)]
    pub struct RearrangedFieldOrder {
        #[prost(bytes, tag = "1")]
        pub bytes: Vec<u8>,
        #[prost(uint32, tag = "2")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "3")]
        pub items: Vec<u32>,
        #[prost(uint32, tag = "4")]
        pub price: u32,
    }

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
        let data = flight_test::<ArconDataTest>(FlightSerde::Reliable);
        for d in data {
            assert_eq!(d.data.items, *ITEMS);
            assert_eq!(d.data.price, PRICE);
            assert_eq!(d.data.id, ID);
        }
    }

    #[cfg(feature = "unsafe_flight")]
    #[test]
    fn unsafe_serde_test() {
        let data = flight_test::<ArconDataTest>(FlightSerde::Unsafe);
        for d in data {
            assert_eq!(d.data.items, *ITEMS);
            assert_eq!(d.data.price, PRICE);
            assert_eq!(d.data.id, ID);
        }
    }

    #[cfg(feature = "unsafe_flight")]
    #[test]
    #[should_panic]
    fn unsafe_version_mismatch_test() {
        // Should panic on ArconType version mismatch
        // Expected Version 1 but Received Version 2.
        let _ = flight_test::<UpdatedVer>(FlightSerde::Unsafe);
    }

    #[cfg(feature = "unsafe_flight")]
    #[test]
    #[should_panic]
    fn serde_id_mismatch_test() {
        // Should panic with Unexpected Deserializer.
        // reliable/unsafe ser_ids do not match
        // NOTE: does not matter whether it is Unsafe/Reliable
        let _ = flight_test::<UpdatedSerId>(FlightSerde::Unsafe);
    }

    #[test]
    fn reliable_added_field_test() {
        let data = flight_test::<AddedField>(FlightSerde::Reliable);
        for d in data {
            assert_eq!(d.data.items, *ITEMS);
            assert_eq!(d.data.price, PRICE);
            assert_eq!(d.data.id, ID);
            assert_eq!(d.data.bytes.len(), 0);
        }
    }

    #[test]
    #[should_panic]
    fn reliable_rearranged_field_test() {
        // This will fail to deserialize and thus panic
        let _ = flight_test::<RearrangedFieldOrder>(FlightSerde::Reliable);
    }

    #[test]
    fn reliable_removed_field_test() {
        // Protobuf will still be able to deserialize the data.
        // Ignoring the sent price field
        let data = flight_test::<RemovedField>(FlightSerde::Reliable);
        for d in data {
            assert_eq!(d.data.id, ID);
            assert_eq!(d.data.items, *ITEMS);
        }
    }

    fn flight_test<ReceivingType>(serde: FlightSerde) -> Vec<ArconElement<ReceivingType>>
    where
        ReceivingType: ArconType,
    {
        let pipeline = Pipeline::default();
        let pool_info = pipeline.get_pool_info();
        let (local, remote) = get_systems();
        let timeout = Duration::from_millis(150);
        let comp = remote.create(DebugNode::<ReceivingType>::new);
        remote
            .start_notify(&comp)
            .wait_timeout(timeout)
            .expect("comp never started");

        let comp_id = String::from("comp");
        let reg = remote.register_by_alias(&comp, comp_id.clone());
        reg.wait_expect(
            Duration::from_millis(1000),
            "Failed to register alias for DebugNode",
        );
        let remote_path =
            ActorPath::Named(NamedPath::with_system(remote.system_path(), vec![comp_id]));

        let channel = Channel::Remote(remote_path, serde);
        let mut channel_strategy: ChannelStrategy<ArconDataTest> =
            ChannelStrategy::Forward(Forward::new(channel, 1.into(), pool_info));

        let data = ArconDataTest {
            id: ID,
            items: ITEMS.clone(),
            price: PRICE,
        };
        let element = ArconElement::new(data);

        comp.on_definition(|cd| {
            channel_strategy.add(ArconEvent::Element(element.clone()), cd);
            channel_strategy.add(ArconEvent::Element(element.clone()), cd);
            channel_strategy.add(ArconEvent::Element(element.clone()), cd);
            channel_strategy.add(ArconEvent::Element(element), cd);
            channel_strategy.flush(cd);
        });

        std::thread::sleep(timeout);

        let data = comp.on_definition(|cd| {
            assert_eq!(cd.data.len() as u64, 4);
            cd.data.clone()
        });

        let _ = local.shutdown();
        let _ = remote.shutdown();

        data
    }
}
