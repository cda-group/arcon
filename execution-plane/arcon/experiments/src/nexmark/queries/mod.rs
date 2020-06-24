// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{
    config::NEXMarkConfig,
    sink::{NEXMarkSink, Run, SinkPort},
    source::NEXMarkSource,
    NEXMarkEvent,
};

use arcon::{pipeline::CreatedDynamicNode, prelude::*, timer::TimerBackend};
use std::{rc::Rc, time::Duration};

pub mod q1;
pub mod q3;

type QueryTimer = Option<KFuture<QueryResult>>;
type StateMetricsThief<T> = Rc<dyn Fn(CreatedDynamicNode<T>) -> Option<state::metered::Metrics>>;
type StateMetricsPrinter = Box<dyn FnOnce()>;

// `node` is important for type inference! some of the types here can potentially be unnameable!
fn make_state_metrics_thief<IN, O, B, T>(_node: &Node<O, B, T>) -> StateMetricsThief<IN>
where
    IN: ArconType,
    B: state::Backend,
    O: Operator<B>,
    T: TimerBackend<O::TimerState>,
{
    Rc::new(|dyn_node: CreatedDynamicNode<IN>| {
        dyn_node
            .as_any()
            .downcast_ref::<Component<Node<O, B, T>>>()
            .unwrap()
            .on_definition(|n| {
                n.state_backend.inner.get_mut().metrics().map(|m| {
                    // we leave vastly shorter metrics in place of the ones we stole
                    std::mem::replace(m, state::metered::Metrics::new(256))
                })
            })
    })
}

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
        state_backend_type: state::BackendType,
    ) -> (QueryTimer, Vec<StateMetricsPrinter>);
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

pub fn source<OP, B, T>(
    sink_port_opt: Option<ProvidedRef<SinkPort>>,
    nexmark_config: NEXMarkConfig,
    source_context: SourceContext<OP, B, T>,
    system: &mut KompactSystem,
) -> QueryTimer
where
    OP: Operator<B, IN = NEXMarkEvent, TimerState = ArconNever> + 'static,
    B: state::Backend,
    T: TimerBackend<ArconNever>,
{
    let nexmark_source_comp =
        system.create_dedicated(move || NEXMarkSource::new(nexmark_config, source_context));

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
