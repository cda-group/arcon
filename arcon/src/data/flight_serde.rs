/// An Enum holding possible serialisation/deserialisation options for in-flight data
#[derive(Clone)]
pub enum FlightSerde {
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

    impl<A: ArconType> Deserialiser<RawArconMessage<A>> for ReliableSerde<A> {
        const SER_ID: SerId = A::RELIABLE_SER_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<RawArconMessage<A>, SerError> {
            RawArconMessage::decode(buf.chunk()).map_err(|e| SerError::InvalidData(e.to_string()))
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::stream::channel::strategy::send;
    use crate::{
        application::Application,
        data::{ArconElement, ArconEvent, ArconType, Watermark},
        stream::{
            channel::{
                strategy::{forward::Forward, ChannelStrategy},
                Channel,
            },
            node::debug::DebugNode,
        },
    };
    use kompact::prelude::*;
    use once_cell::sync::Lazy;
    use std::time::Duration;

    static ITEMS: Lazy<Vec<u32>> = Lazy::new(|| vec![1, 2, 3, 4, 5, 6, 7]);
    const PRICE: u32 = 10;
    const ID: u32 = 1;

    // The flight_serde application will always send data of ArconDataTest
    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(reliable_ser_id = 105, version = 1)]
    pub struct ArconDataTest {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
        #[prost(uint32, tag = "3")]
        pub price: u32,
    }

    // Down below are different structs the unit tests may attempt to deserialise into

    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(reliable_ser_id = 105, version = 2)]
    pub struct UpdatedVer {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
        #[prost(uint32, tag = "3")]
        pub price: u32,
    }

    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(reliable_ser_id = 205, version = 2)]
    pub struct UpdatedSerId {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
        #[prost(uint32, tag = "3")]
        pub price: u32,
    }

    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(reliable_ser_id = 105, version = 1)]
    pub struct RemovedField {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
    }

    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(reliable_ser_id = 105, version = 1)]
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

    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(reliable_ser_id = 105, version = 1)]
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
            let mut cfg = KompactConfig::default();
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
        let app = Application::default();
        let pool_info = app.get_pool_info();
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
            channel_strategy.push(ArconEvent::Element(element.clone()));
            channel_strategy.push(ArconEvent::Element(element.clone()));
            channel_strategy.push(ArconEvent::Element(element.clone()));
            channel_strategy.push(ArconEvent::Element(element));

            // force a flush through a marker
            for (channel, msg) in channel_strategy.push(ArconEvent::Watermark(Watermark::new(0))) {
                let _ = send(&channel, msg, cd);
            }
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
