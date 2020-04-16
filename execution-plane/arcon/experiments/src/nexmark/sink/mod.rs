// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon::prelude::*;
use std::time::Duration;

#[derive(Debug)]
pub struct Run {
    promise: KPromise<Duration>,
}
impl Run {
    pub fn new(promise: KPromise<Duration>) -> Run {
        Run { promise }
    }
}

impl Clone for Run {
    fn clone(&self) -> Self {
        unimplemented!("Shouldn't be invoked in this experiment!");
    }
}

pub struct SinkPort;
impl Port for SinkPort {
    type Indication = ();
    type Request = Run;
}

/// Custom Sink for NEXMark
#[derive(ComponentDefinition)]
#[allow(dead_code)]
pub struct NEXMarkSink<IN>
where
    IN: ArconType,
{
    ctx: ComponentContext<Self>,
    pub sink_port: ProvidedPort<SinkPort, Self>,
    done: Option<KPromise<Duration>>,
    start: std::time::Instant,
}

impl<IN> NEXMarkSink<IN>
where
    IN: ArconType,
{
    pub fn new() -> Self {
        NEXMarkSink {
            ctx: ComponentContext::new(),
            sink_port: ProvidedPort::new(),
            done: None,
            start: std::time::Instant::now(),
        }
    }
}

impl<IN> Provide<ControlPort> for NEXMarkSink<IN>
where
    IN: ArconType,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {}
    }
}

impl<IN> Provide<SinkPort> for NEXMarkSink<IN>
where
    IN: ArconType,
{
    fn handle(&mut self, event: Run) -> () {
        self.done = Some(event.promise);
        self.start = std::time::Instant::now();
    }
}

impl<IN> Actor for NEXMarkSink<IN>
where
    IN: ArconType,
{
    type Message = ArconMessage<IN>;
    fn receive_local(&mut self, msg: Self::Message) {
        for msg in msg.events {
            match msg.unwrap() {
                ArconEvent::Death(_) => {
                    let time = self.start.elapsed();
                    let promise = self.done.take().expect("No promise to reply to?");
                    promise.fulfill(time).expect("Promise was dropped");
                }
                _ => {}
            }
        }
    }
    fn receive_network(&mut self, _msg: NetMessage) {}
}
