use crate::data::{flight_serde::reliable_remote::ReliableSerde, *};
use kompact::prelude::*;
use std::collections::HashSet;

/// A DebugNode is a debug version of [Node]
///
/// DebugNode's act as sinks and are useful for tests and situations
/// when one needs to verify dev pipelines.
#[derive(ComponentDefinition)]
pub struct DebugNode<IN>
where
    IN: ArconType,
{
    ctx: ComponentContext<DebugNode<IN>>,
    /// Buffer holding all received [ArconElement]
    pub data: Vec<ArconElement<IN>>,
    /// Buffer holding all received [Watermark]
    pub watermarks: Vec<Watermark>,
    /// Buffer holding all received [Epoch]
    pub epochs: Vec<Epoch>,
    /// Buffer holding all NodeID's the DebugNode has received messages from
    pub senders: HashSet<NodeID>,
}

impl<IN> DebugNode<IN>
where
    IN: ArconType,
{
    pub fn new() -> DebugNode<IN> {
        DebugNode {
            ctx: ComponentContext::uninitialised(),
            data: Vec::new(),
            watermarks: Vec::new(),
            epochs: Vec::new(),
            senders: HashSet::new(),
        }
    }
    #[inline]
    fn handle_events<I>(&mut self, events: I)
    where
        I: IntoIterator<Item = ArconEventWrapper<IN>>,
    {
        for event in events.into_iter() {
            match event.unwrap() {
                ArconEvent::Element(e) => {
                    info!(self.ctx.log(), "Sink element: {:?}", e.data);
                    self.data.push(e);
                }
                ArconEvent::Watermark(w) => {
                    self.watermarks.push(w);
                }
                ArconEvent::Epoch(e) => {
                    self.epochs.push(e);
                }
                ArconEvent::Death(_) => {}
            }
        }
    }
}

impl<IN> Default for DebugNode<IN>
where
    IN: ArconType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<IN> ComponentLifecycle for DebugNode<IN>
where
    IN: ArconType,
{
    fn on_start(&mut self) -> Handled {
        debug!(self.ctx.log(), "Started Arcon DebugNode");
        Handled::Ok
    }
}

impl<IN> Actor for DebugNode<IN>
where
    IN: ArconType,
{
    type Message = ArconMessage<IN>;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.senders.insert(msg.sender);
        self.handle_events(msg.events);
        Handled::Ok
    }
    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        let arcon_msg = match *msg.ser_id() {
            id if id == IN::RELIABLE_SER_ID => msg
                .try_deserialise::<RawArconMessage<IN>, ReliableSerde<IN>>()
                .unwrap(),
            _ => {
                panic!("Unexpected deserialiser")
            }
        };
        self.senders.insert(arcon_msg.sender);
        self.handle_events(arcon_msg.events);
        Handled::Ok
    }
}
