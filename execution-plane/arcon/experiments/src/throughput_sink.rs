// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon::prelude::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

// Throughput logic taken from: https://github.com/lsds/StreamBench/blob/master/yahoo-streaming-benchmark/src/main/scala/uk/ac/ic/imperial/benchmark/flink/YahooBenchmark.scala
#[derive(ComponentDefinition)]
pub struct ThroughputSink<A>
where
    A: ArconType,
{
    ctx: ComponentContext<Self>,
    pub sink_port: ProvidedPort<SinkPort, Self>,
    done: Option<KPromise<Duration>>,
    start: std::time::Instant,
    log_on: bool,
    expected_msgs: u64,
    log_freq: u64,
    last_total_recv: u64,
    last_time: u64,
    total_recv: u64,
    avg_throughput: f32,
    throughput_counter: u64,
    throughput_sum: f32,
}

impl<A> ThroughputSink<A>
where
    A: ArconType,
{
    pub fn new(log_freq: u64, log_on: bool, expected_msgs: u64) -> Self {
        ThroughputSink {
            ctx: ComponentContext::new(),
            sink_port: ProvidedPort::new(),
            done: None,
            start: std::time::Instant::now(),
            log_on,
            expected_msgs,
            log_freq,
            last_total_recv: 0,
            last_time: 0,
            total_recv: 0,
            avg_throughput: 0.0,
            throughput_counter: 0,
            throughput_sum: 0.0,
        }
    }

    #[inline(always)]
    fn handle_msg(&mut self, msg: ArconMessage<A>) {
        if self.total_recv == 0 {
            if self.log_on {
                info!(
                    self.ctx.log(),
                    "ThroughputLogging {}, {}",
                    self.get_current_time(),
                    self.total_recv
                );
            }
        }

        for _event in msg.events {
            self.total_recv += 1;

            if self.total_recv % self.log_freq == 0 {
                let current_time = self.get_current_time();
                let throughput = (self.total_recv - self.last_total_recv)
                    / (current_time - self.last_time)
                    * 1000;
                if throughput != 0 {
                    self.throughput_counter += 1;
                    self.throughput_sum += throughput as f32;
                    self.avg_throughput = self.throughput_sum / self.throughput_counter as f32;
                }

                if self.log_on {
                    info!(
                        self.ctx.log(),
                        "Throughput {}, Average {}", throughput, self.avg_throughput
                    );

                    info!(
                        self.ctx.log(),
                        "ThroughputLogging {}, {}",
                        self.get_current_time(),
                        self.total_recv
                    );
                }
                self.last_time = current_time;
                self.last_total_recv = self.total_recv;
            }

            if self.total_recv == self.expected_msgs {
                let time = self.start.elapsed();
                let promise = self.done.take().expect("No promise to reply to?");
                promise.fulfil(time).expect("Promise was dropped");
            }
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

impl<A> Provide<ControlPort> for ThroughputSink<A>
where
    A: ArconType,
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl<A> Actor for ThroughputSink<A>
where
    A: ArconType,
{
    type Message = ArconMessage<A>;

    fn receive_local(&mut self, msg: Self::Message) {
        self.handle_msg(msg);
    }

    fn receive_network(&mut self, _msg: NetMessage) {
        panic!("only local exec");
    }
}

impl<A> Provide<SinkPort> for ThroughputSink<A>
where
    A: ArconType + 'static,
{
    fn handle(&mut self, event: Run) -> () {
        self.done = Some(event.promise);
        self.start = std::time::Instant::now();
    }
}
