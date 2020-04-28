// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::*;

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
}

impl<IN> DebugNode<IN>
where
    IN: ArconType,
{
    pub fn new() -> DebugNode<IN> {
        DebugNode {
            ctx: ComponentContext::new(),
            data: Vec::new(),
            watermarks: Vec::new(),
            epochs: Vec::new(),
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

impl<IN> Provide<ControlPort> for DebugNode<IN>
where
    IN: ArconType,
{
    fn handle(&mut self, event: ControlEvent) {
        match event {
            ControlEvent::Start => {
                debug!(self.ctx.log(), "Started Arcon DebugNode");
            }
            ControlEvent::Stop => {
                // TODO
            }
            ControlEvent::Kill => {
                // TODO
            }
        }
    }
}

impl<IN> Actor for DebugNode<IN>
where
    IN: ArconType,
{
    type Message = ArconMessage<IN>;

    fn receive_local(&mut self, msg: Self::Message) {
        self.handle_events(msg.events);
    }
    fn receive_network(&mut self, msg: NetMessage) {
        let unsafe_id = IN::UNSAFE_SER_ID;
        let reliable_id = IN::RELIABLE_SER_ID;
        let ser_id = *msg.ser_id();

        let arcon_msg = {
            if ser_id == reliable_id {
                msg.try_deserialise::<RawArconMessage<IN>, ReliableSerde<IN>>()
                    .map_err(|_| arcon_err_kind!("Failed to unpack reliable ArconMessage"))
            } else if ser_id == unsafe_id {
                msg.try_deserialise::<RawArconMessage<IN>, UnsafeSerde<IN>>()
                    .map_err(|_| arcon_err_kind!("Failed to unpack unreliable ArconMessage"))
            } else {
                panic!("Unexpected deserialiser")
            }
        };

        match arcon_msg {
            Ok(m) => {
                self.handle_events(m.events);
            }
            Err(e) => error!(self.ctx.log(), "Error ArconNetworkMessage: {:?}", e),
        }
    }
}
