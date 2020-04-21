// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{
    config::NEXMarkConfig,
    sink::{NEXMarkSink, Run, SinkPort},
    source::NEXMarkSource,
    NEXMarkEvent,
};

use arcon::prelude::*;
use std::time::Duration;

pub mod q1;
pub mod q3;

type QueryTimer = Option<KFuture<QueryResult>>;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub timer: Duration,
    pub events_per_sec: f64,
}
impl QueryResult {
    pub fn new(timer: Duration, events_per_sec: f64) -> Self {
        QueryResult {
            timer,
            events_per_sec,
        }
    }
}

pub trait Query {
    fn run(
        debug_mode: bool,
        nexmark_config: NEXMarkConfig,
        pipeline: &mut ArconPipeline,
    ) -> QueryTimer;
}

pub fn sink<A: ArconType>(
    debug_mode: bool,
    system: &mut KompactSystem,
) -> (
    ActorRefStrong<ArconMessage<A>>,
    Option<ProvidedRef<SinkPort>>,
) {
    let timeout = std::time::Duration::from_millis(500);
    // If debug mode is enabled, we send the data to a DebugNode.
    // Otherwise, just send the data to the NEXMarkSink...
    if debug_mode {
        let sink = system.create(move || DebugNode::<A>::new());
        system
            .start_notify(&sink)
            .wait_timeout(timeout)
            .expect("sink never started!");
        (sink.actor_ref().hold().expect("no"), None)
    } else {
        let sink = system.create(move || NEXMarkSink::<A>::new());
        system
            .start_notify(&sink)
            .wait_timeout(timeout)
            .expect("sink never started!");
        let sink_port = sink.on_definition(|cd| cd.sink_port.share());
        (sink.actor_ref().hold().expect("no"), Some(sink_port))
    }
}

pub fn source<OUT: ArconType>(
    sink_port_opt: Option<ProvidedRef<SinkPort>>,
    nexmark_config: NEXMarkConfig,
    source_context: SourceContext<NEXMarkEvent, OUT>,
    system: &mut KompactSystem,
) -> QueryTimer {
    let nexmark_source_comp =
        system.create_dedicated(move || NEXMarkSource::<OUT>::new(nexmark_config, source_context));

    // NOTE: processing starts as soon as source starts,
    // so first start set up execution time at the sink if
    // we are not running in debug mode.
    let fn_return = {
        if let Some(sink_port) = sink_port_opt {
            let (promise, future) = kpromise();
            system.trigger_r(Run::new(promise), &sink_port);
            Some(future)
        } else {
            None
        }
    };

    system.start(&nexmark_source_comp);

    fn_return
}
