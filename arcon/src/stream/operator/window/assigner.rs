use super::WindowContext;
use crate::dataflow::{api::Assigner, conf::WindowConf};
use crate::index::WindowIndex;
use crate::prelude::EagerValue;
use crate::{
    data::ArconElement,
    error::*,
    index::{EagerHashTable, IndexOps, ValueIndex},
    reportable_error,
    stream::operator::{Operator, OperatorContext},
};
use arcon_macros::ArconState;
use arcon_state::Backend;
use kompact::prelude::error;

use prost::Message;
use std::{marker::PhantomData, sync::Arc};

type Key = u64;
type Index = u64;
type Timestamp = u64;

#[derive(Message, PartialEq, Clone, Copy)]
pub struct WindowEvent {
    #[prost(uint64, tag = "1")]
    key: Key,
    #[prost(uint64, tag = "2")]
    index: Index,
    #[prost(uint64, tag = "3")]
    timestamp: Timestamp,
}

impl WindowEvent {
    fn new(key: Key, index: Index, timestamp: Timestamp) -> WindowEvent {
        WindowEvent {
            key,
            index,
            timestamp,
        }
    }
}

#[derive(ArconState)]
pub struct WindowState<I: WindowIndex, B: Backend> {
    window_start: EagerValue<Timestamp, B>,
    active_windows: EagerHashTable<WindowContext, (), B>,
    index: I,
}

impl<I: WindowIndex, B: Backend> WindowState<I, B> {
    pub fn new(index: I, backend: Arc<B>) -> Self {
        Self {
            window_start: EagerValue::new("_window_start", backend.clone()),
            active_windows: EagerHashTable::new("_active_windows", backend),
            index,
        }
    }
}

/// Window Assigner Based on Event Time
pub struct WindowAssigner<I, B>
where
    I: WindowIndex,
    B: Backend,
{
    // effectively immutable, so no reason to persist
    window_length: u64,
    window_slide: u64,
    late_arrival_time: u64,
    _marker: PhantomData<(I, B)>,
}

impl<I, B> WindowAssigner<I, B>
where
    I: WindowIndex,
    B: Backend,
{
    pub fn new(conf: WindowConf) -> Self {
        match conf.assigner {
            Assigner::Sliding {
                length,
                slide,
                late_arrival,
            } => Self::setup(length.0, slide.0, late_arrival.0),
            Assigner::Tumbling {
                length,
                late_arrival,
            } => Self::setup(length.0, length.0, late_arrival.0),
        }
    }

    // Setup method for both sliding and tumbling windows
    fn setup(length: u64, slide: u64, late: u64) -> Self {
        // Sanity check on slide and length
        if length < slide {
            panic!("Window Length lower than slide!");
        }
        if length % slide != 0 {
            panic!("Window Length not divisible by slide!");
        }

        WindowAssigner {
            window_length: length,
            window_slide: slide,
            late_arrival_time: late,
            _marker: Default::default(),
        }
    }

    #[inline]
    fn new_window_trigger(
        &mut self,
        window_ctx: WindowContext,
        ctx: &mut OperatorContext<WindowEvent, WindowState<I, B>>,
    ) -> ArconResult<()> {
        let window_start = match ctx.state().window_start().get()? {
            Some(start) => *start,
            None => {
                return reportable_error!(
                    "Unexpected failure, could not find window start for existing key"
                );
            }
        };

        let ts = window_start + (window_ctx.index * self.window_slide) + self.window_length;

        let request = ctx.schedule_at(
            ts + self.late_arrival_time,
            WindowEvent::new(window_ctx.key, window_ctx.index, ts),
        )?;

        if let Err(expired) = request {
            // For now just log the error..
            error!(ctx.log(), "{}", expired);
        }
        Ok(())
    }
}

impl<I, B> Operator for WindowAssigner<I, B>
where
    I: WindowIndex,
    B: Backend,
{
    type IN = I::IN;
    type OUT = I::OUT;
    type TimerState = WindowEvent;
    type OperatorState = WindowState<I, B>;
    type ElementIterator = Option<ArconElement<I::OUT>>;

    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Self::ElementIterator> {
        let ts = element.timestamp;

        let time = ctx.current_time()?;

        let ts_lower_bound = time.saturating_sub(self.late_arrival_time);

        if ts < ts_lower_bound {
            // Late arrival: early return
            return Ok(None);
        }

        let start = match ctx.state().window_start().get()? {
            Some(start) => *start,
            None => {
                if ts < self.late_arrival_time {
                    0
                } else {
                    let start = ts - self.late_arrival_time;
                    ctx.state().window_start().put(start)?;
                    start
                }
            }
        };
        let ceil = (ts - start) / self.window_slide;
        let floor = if ceil >= (self.window_length / self.window_slide) {
            ceil - (self.window_length / self.window_slide) + 1
        } else {
            0
        };

        // For all windows, insert element....
        for index in floor..=ceil {
            let window_ctx = WindowContext {
                key: ctx.current_key,
                index,
            };
            ctx.state()
                .index()
                .on_element(element.data.clone(), window_ctx)?;

            let active_exist = ctx.state().active_windows().contains(&window_ctx)?;

            // if it does not exist, then add active window and create trigger
            if !active_exist {
                ctx.state().active_windows().put(window_ctx, ())?;

                self.new_window_trigger(window_ctx, ctx)?;
            }
        }

        Ok(None)
    }

    fn handle_timeout(
        &mut self,
        timeout: Self::TimerState,
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Option<Self::ElementIterator>> {
        let WindowEvent {
            key,
            index,
            timestamp,
        } = timeout;
        ctx.current_key = key;
        let window_ctx = WindowContext::new(key, index);

        let state = ctx.state();
        let result = state.index().result(window_ctx)?;
        state.index().clear(window_ctx)?;
        state.active_windows().remove(&window_ctx)?;

        Ok(Some(Some(ArconElement::with_timestamp(result, timestamp))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "hardware_counters")]
    #[cfg(not(test))]
    use crate::metrics::perf_event::{HardwareCounter, PerfEvents};
    use crate::{
        application::*,
        data::{ArconMessage, NodeID},
        dataflow::dfg::GlobalNodeId,
        index::AppenderWindow,
        manager::node::{NodeManager, NodeManagerPort},
        prelude::{KeyBuilder, OperatorBuilder},
        stream::{
            channel::{
                strategy::{forward::Forward, ChannelStrategy},
                Channel,
            },
            node::{debug::DebugNode, Node, NodeState},
            time::Time,
        },
    };
    use arcon_state::Sled;
    use kompact::prelude::{biconnect_components, ActorRefFactory, ActorRefStrong, Component};
    use std::{sync::Arc, thread, time, time::UNIX_EPOCH};

    // helper functions
    fn window_assigner_test_setup(
        length: u64,
        slide: u64,
        late: u64,
        keyed: bool,
    ) -> (
        ActorRefStrong<ArconMessage<u64>>,
        Arc<Component<DebugNode<u64>>>,
    ) {
        let app = AssembledApplication::default();
        let pool_info = app.app.get_pool_info();
        let epoch_manager_ref = app.epoch_manager();

        // Create a sink
        let sink = app.data_system().create(DebugNode::<u64>::new);

        app.data_system()
            .start_notify(&sink)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        let sink_ref: ActorRefStrong<ArconMessage<u64>> =
            sink.actor_ref().hold().expect("failed to get strong ref");

        let channel_strategy = ChannelStrategy::Forward(Forward::new(
            Channel::Local(sink_ref),
            NodeID::new(1),
            pool_info,
        ));

        let backend = Arc::new(crate::test_utils::temp_backend::<Sled>());
        let descriptor = String::from("node_");
        let in_channels = vec![0.into()];

        fn appender_fn(u: &[u64]) -> u64 {
            u.len() as u64
        }

        let builder = OperatorBuilder {
            operator: Arc::new(move || {
                let conf = WindowConf {
                    assigner: Assigner::Sliding {
                        length: Time::seconds(length),
                        slide: Time::seconds(slide),
                        late_arrival: Time::seconds(late),
                    },
                };
                WindowAssigner::new(conf)
            }),
            state: Arc::new(|backend: Arc<Sled>| {
                let index = AppenderWindow::new(backend.clone(), &appender_fn);
                WindowState::new(index, backend)
            }),
            conf: Default::default(),
        };

        let operator = builder.operator.clone();
        let operator_state = builder.state.clone();

        let nm = NodeManager::new(
            descriptor.clone(),
            app.data_system().clone(),
            in_channels.clone(),
            app.app.arcon_logger.clone(),
            Arc::new(builder),
        );

        let node_manager_comp = app.ctrl_system().create(|| nm);

        app.ctrl_system()
            .start_notify(&node_manager_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        #[cfg(feature = "hardware_counters")]
        #[cfg(not(test))]
        let mut perf_events = PerfEvents::new();

        let key_builder: Option<KeyBuilder<u64>> = if keyed {
            Some(KeyBuilder {
                extractor: Arc::new(|d: &u64| *d),
            })
        } else {
            None
        };

        let node: Node<_, _> = Node::new(
            descriptor,
            channel_strategy,
            operator(),
            operator_state(backend.clone()),
            NodeState::new(NodeID::new(0), in_channels, backend.clone()),
            backend,
            app.app.arcon_logger.clone(),
            epoch_manager_ref,
            #[cfg(feature = "hardware_counters")]
            #[cfg(not(test))]
            perf_events,
            GlobalNodeId::null(),
            key_builder,
        );

        let window_comp = app.data_system().create(|| node);
        let required_ref = window_comp.on_definition(|cd| cd.node_manager_port.share());

        biconnect_components::<NodeManagerPort, _, _>(&node_manager_comp, &window_comp)
            .expect("connection");

        app.data_system()
            .start_notify(&window_comp)
            .wait_timeout(std::time::Duration::from_millis(1000))
            .expect("started");

        let win_ref: ActorRefStrong<ArconMessage<u64>> = window_comp
            .actor_ref()
            .hold()
            .expect("failed to get strong ref");

        node_manager_comp.on_definition(|cd| {
            // Insert the created Node into the NodeManager
            cd.nodes
                .insert(GlobalNodeId::null(), (window_comp, required_ref));
        });

        (win_ref, sink)
    }
    fn now() -> u64 {
        time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("error")
            .as_secs()
    }
    fn wait(time: u64) {
        thread::sleep(time::Duration::from_secs(time));
    }
    fn watermark(time: u64) -> ArconMessage<u64> {
        ArconMessage::watermark(time, 0.into())
    }
    fn timestamped_event(ts: u64) -> ArconMessage<u64> {
        ArconMessage::element(1u64, ts, 0.into())
    }
    fn timestamped_keyed_event(ts: u64, id: u64) -> ArconMessage<u64> {
        ArconMessage::element(id, ts, 0.into())
    }

    // Tests:
    #[test]
    fn window_by_key() {
        let (assigner_ref, sink) = window_assigner_test_setup(10, 5, 0, true);
        wait(1);
        let moment = now();
        assigner_ref.tell(timestamped_keyed_event(moment, 1));
        assigner_ref.tell(timestamped_keyed_event(moment + 1, 2));
        assigner_ref.tell(timestamped_keyed_event(moment + 2, 3));
        assigner_ref.tell(timestamped_keyed_event(moment + 3, 2));
        assigner_ref.tell(timestamped_keyed_event(moment + 5, 2));
        assigner_ref.tell(timestamped_keyed_event(moment + 4, 1));

        wait(1);
        assigner_ref.tell(watermark(moment + 12));
        wait(2);
        sink.on_definition(|cd| {
            let r1 = &cd.data.len();
            assert_eq!(r1, &3); // 3 windows received
            let r2 = &cd.data[0].data;
            assert_eq!(r2, &2); // 1st window for key 1 has 2 elements
            let r3 = &cd.data[1].data;
            assert_eq!(r3, &3); // 2nd window receieved, key 2, has 3 elements
            let r4 = &cd.data[2].data;
            assert_eq!(r4, &1); // 3rd window receieved, for key 3, has 1 elements
        });
    }

    #[test]
    fn window_discard_late_arrival() {
        // Send 2 messages on time, a watermark and a late arrival which is not allowed

        let (assigner_ref, sink) = window_assigner_test_setup(10, 10, 0, false);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(watermark(moment + 10));
        wait(1);
        assigner_ref.tell(timestamped_event(moment));
        wait(1);
        // Inspect and assert
        sink.on_definition(|cd| {
            let r1 = &cd.data[0].data;
            assert_eq!(r1, &2);
            let r2 = &cd.data.len();
            assert_eq!(r2, &1);
        });
    }
    #[test]
    fn window_too_late_late_arrival() {
        // Send 2 messages on time, and then 1 message which is too late
        let (assigner_ref, sink) = window_assigner_test_setup(10, 10, 10, false);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(watermark(moment + 21));
        wait(1);
        assigner_ref.tell(timestamped_event(moment));
        wait(1);
        // Inspect and assert
        sink.on_definition(|cd| {
            let w0 = &cd.data[0].data;
            let w0_ts = &cd.data[0].timestamp;
            assert_eq!(&cd.data.len(), &(1_usize));
            assert_eq!(w0, &2);
            assert_eq!(w0_ts, &(moment + 10));
        });
    }
    #[test]
    fn window_allow_late_arrival() {
        // Send 2 messages on time, and then 1 message which is too late
        let (assigner_ref, sink) = window_assigner_test_setup(10, 10, 10, false);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(timestamped_event(moment - 5));
        assigner_ref.tell(watermark(moment + 21));
        wait(1);
        wait(1);
        // Inspect and assert
        sink.on_definition(|cd| {
            let w0 = &cd.data[0].data;
            let w0_ts = &cd.data[0].timestamp;
            let w1 = &cd.data[1].data;
            let w1_ts = &cd.data[1].timestamp;
            assert_eq!(&cd.data.len(), &(2_usize));
            assert_eq!(w0, &1);
            assert_eq!(w0_ts, &(moment));
            assert_eq!(w1, &2);
            assert_eq!(w1_ts, &(moment + 10));
        });
    }
    #[test]
    fn window_very_long_windows_1() {
        // Use long windows to check for timer not going out of sync in ms conversion
        let (assigner_ref, sink) = window_assigner_test_setup(10000, 10000, 0, false);
        wait(1);
        let moment = now();

        // Spawns first window
        assigner_ref.tell(timestamped_event(moment));

        // Spawns second window
        assigner_ref.tell(timestamped_event(moment + 10001));

        // Should only materialize first window
        assigner_ref.tell(watermark(moment + 19999));
        wait(2);
        sink.on_definition(|cd| {
            let r0 = &cd.data[0].data;
            assert_eq!(r0, &1);
            assert_eq!(&cd.data.len(), &(1_usize));
        });
    }
    #[test]
    fn window_very_long_windows_2() {
        // Use long windows to check for timer not going out of alignment
        let (assigner_ref, sink) = window_assigner_test_setup(10000, 10000, 0, false);
        wait(1);
        let moment = now();

        // Spawns first window
        assigner_ref.tell(timestamped_event(moment));

        // Spawns second window
        assigner_ref.tell(timestamped_event(moment + 10001));

        // Should only materialize first window
        assigner_ref.tell(watermark(moment + 20000));
        wait(2);
        sink.on_definition(|cd| {
            let r0 = &cd.data[0].data;
            assert_eq!(r0, &1);
            assert_eq!(&cd.data.len(), &(2_usize));
            let r1 = &cd.data[1].data;
            assert_eq!(r1, &1);
        });
    }
    #[test]
    fn window_overlapping() {
        // Use overlapping windows (slide = length/2), check that messages appear correctly
        let (assigner_ref, sink) = window_assigner_test_setup(10, 5, 2, false);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(timestamped_event(moment + 6));
        assigner_ref.tell(timestamped_event(moment + 6));
        assigner_ref.tell(watermark(moment + 23));
        wait(2);
        // Inspect and assert
        sink.on_definition(|cd| {
            //let r2 = &cd.data.len();
            //assert_eq!(r2, &2);
            let r0 = &cd.data[0].data;
            assert_eq!(r0, &3);
            let r1 = &cd.data[1].data;
            assert_eq!(r1, &2);
        });
    }
    #[test]
    fn window_empty() {
        // check that we receive correct number windows from fast forwarding
        let (assigner_ref, sink) = window_assigner_test_setup(5, 5, 0, false);
        wait(1);
        assigner_ref.tell(watermark(now() + 1));
        assigner_ref.tell(watermark(now() + 7));
        wait(1);
        sink.on_definition(|cd| {
            // The number of windows is hard to assert with dynamic window starts
            //assert_eq!(&sink_inspect.data.len(), &(1 as usize));
            // We should've receieved at least one window which is empty
            let r0 = &cd.data.len();
            assert_eq!(r0, &0);
        });
    }
}
