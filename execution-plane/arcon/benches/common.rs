// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon::prelude::*;
use arcon_error::*;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Run {
    num_iterations: u64,
    promise: KPromise<Duration>,
}
impl Run {
    pub fn new(num_iterations: u64, promise: KPromise<Duration>) -> Run {
        Run {
            num_iterations,
            promise,
        }
    }
}
impl Clone for Run {
    fn clone(&self) -> Self {
        unimplemented!("Shouldn't be invoked in this experiment!");
    }
}

pub struct ExperimentPort;
impl Port for ExperimentPort {
    type Indication = ();
    type Request = Run;
}

#[derive(ComponentDefinition)]
pub struct NodeReceiver<A: ArconType> {
    ctx: ComponentContext<Self>,
    pub experiment_port: ProvidedPort<ExperimentPort, Self>,
    done: Option<KPromise<Duration>>,
    node: Option<ActorRefStrong<ArconMessage<A>>>,
    remaining_recv: u64,
    start: Instant,
}
impl<A: ArconType> NodeReceiver<A> {
    pub fn new() -> NodeReceiver<A> {
        NodeReceiver {
            ctx: ComponentContext::new(),
            experiment_port: ProvidedPort::new(),
            done: None,
            node: None,
            remaining_recv: 0,
            start: Instant::now(),
        }
    }

    fn handle_msg(&mut self, msg: ArconMessage<A>) {
        self.remaining_recv -= msg.events.len() as u64;
        if self.remaining_recv <= 0u64 {
            let time = self.start.elapsed();
            let promise = self.done.take().expect("No promise to reply to?");
            promise.fulfill(time).expect("Promise was dropped");
        }
    }

    pub fn set_node(&mut self, node: ActorRefStrong<ArconMessage<A>>) {
        self.node = Some(node);
    }
}

impl<A: ArconType> Provide<ControlPort> for NodeReceiver<A> {
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl<A: ArconType> Actor for NodeReceiver<A> {
    type Message = ArconMessage<A>;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        self.handle_msg(msg);
    }

    fn receive_network(&mut self, msg: NetMessage) -> () {
        let arcon_msg: ArconResult<ArconMessage<A>> = match *msg.ser_id() {
            ReliableSerde::<A>::SER_ID => msg
                .try_deserialise::<ArconMessage<A>, ReliableSerde<A>>()
                .map_err(|_| arcon_err_kind!("Failed to unpack reliable ArconMessage")),
            UnsafeSerde::<A>::SER_ID => msg
                .try_deserialise::<ArconMessage<A>, UnsafeSerde<A>>()
                .map_err(|_| arcon_err_kind!("Failed to unpack unreliable ArconMessage")),
            _ => panic!("Unexpected deserialiser"),
        };

        match arcon_msg {
            Ok(m) => {
                self.handle_msg(m);
            }
            Err(e) => error!(self.ctx.log(), "Error NetworkMessage: {:?}", e),
        }
    }
}

impl<A: ArconType> Provide<ExperimentPort> for NodeReceiver<A> {
    fn handle(&mut self, event: Run) -> () {
        self.done = Some(event.promise);
        self.start = Instant::now();
        self.remaining_recv = event.num_iterations;
    }
}
