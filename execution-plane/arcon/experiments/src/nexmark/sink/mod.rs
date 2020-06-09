// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::queries::QueryResult;
use arcon::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Run {
    promise: KPromise<QueryResult>,
}
impl Run {
    pub fn new(promise: KPromise<QueryResult>) -> Run {
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
    pub sink_port: ProvidedPort<SinkPort>,
    done: Option<KPromise<QueryResult>>,
    start: std::time::Instant,
    last_total_recv: u64,
    last_time: u64,
    total_recv: u64,
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
            last_total_recv: 0,
            last_time: 0,
            total_recv: 0,
        }
    }
    #[inline(always)]
    fn get_current_time(&self) -> u64 {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        since_the_epoch.as_millis() as u64
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
        self.last_time = self.get_current_time();
    }
}

impl<IN> Actor for NEXMarkSink<IN>
where
    IN: ArconType,
{
    type Message = ArconMessage<IN>;
    fn receive_local(&mut self, msg: Self::Message) {
        let total_events = msg.events.len();
        self.total_recv += total_events as u64;
        for msg in msg.events {
            match msg.unwrap() {
                ArconEvent::Death(_) => {
                    let time = self.start.elapsed();
                    let current_time = self.get_current_time();
                    let throughput = (self.total_recv - self.last_total_recv)
                        / (current_time - self.last_time)
                        * 1000;
                    let promise = self.done.take().expect("No promise to reply to?");
                    promise
                        .fulfil(QueryResult::new(time, throughput as f64))
                        .expect("Promise was dropped");
                }
                _ => {}
            }
        }
    }
    fn receive_network(&mut self, _msg: NetMessage) {}
}
