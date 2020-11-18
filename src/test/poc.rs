use crate::{
    manager::{
        node::{NodeManager, NodeManagerPort},
        state::{SnapshotRef, StateID},
    },
    prelude::*,
    stream::{node::NodeState, operator::function::Map},
};
use arcon_state::Sled;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[derive(ArconState)]
pub struct MapStateOne<B: Backend> {
    events: Appender<u32, B>,
}

impl<B: Backend> MapStateOne<B> {
    pub fn new(backend: Arc<B>) -> Self {
        let mut events_handle = Handle::vec("_events");
        backend.register_vec_handle(&mut events_handle);

        MapStateOne {
            events: Appender::new(events_handle.activate(backend)),
        }
    }
}

#[derive(ArconState)]
pub struct MapStateTwo<B: Backend> {
    rolling_counter: Value<u64, B>,
}

impl<B: Backend> MapStateTwo<B> {
    pub fn new(backend: Arc<B>) -> Self {
        let mut counter_handle = Handle::value("_counter");
        backend.register_value_handle(&mut counter_handle);

        MapStateTwo {
            rolling_counter: Value::new(counter_handle.activate(backend)),
        }
    }
}

#[derive(ArconState)]
pub struct QueryState<A: Backend, B: Backend> {
    // MapStateOne.events
    events: Appender<u32, A>,
    // MapStateTwo.rolling_counter
    rolling_counter: Value<u64, B>,
}

impl<A: Backend, B: Backend> QueryState<A, B> {
    pub fn new(b1: Arc<A>, b2: Arc<B>) -> Self {
        let mut events_handle = Handle::vec("_events");
        b1.register_vec_handle(&mut events_handle);

        let mut counter_handle = Handle::value("_counter");
        b2.register_value_handle(&mut counter_handle);

        QueryState {
            events: Appender::new(events_handle.activate(b1)),
            rolling_counter: Value::new(counter_handle.activate(b2)),
        }
    }
}

#[test]
fn poc_test() {
    let test_dir = tempfile::tempdir().unwrap();
    let test_dir = test_dir.path();
    let mut conf = ArconConf::default();
    let mut checkpoint_dir: PathBuf = test_dir.into();
    checkpoint_dir.push("checkpoints");
    conf.checkpoint_dir = checkpoint_dir;

    let test_dir_two = tempfile::tempdir().unwrap();
    let test_dir_two = test_dir_two.path();

    let mut pipeline = Pipeline::with_conf(conf);
    let pool_info = pipeline.get_pool_info();
    //let system = &pipeline.system();

    // Set up a Debug Sink
    let sink = pipeline.system().create(DebugNode::<u32>::new);

    pipeline
        .system()
        .start_notify(&sink)
        .wait_timeout(std::time::Duration::from_millis(100))
        .expect("started");

    // Construct Channel to the Debug sink
    let actor_ref: ActorRefStrong<ArconMessage<u32>> =
        sink.actor_ref().hold().expect("Failed to fetch");
    let channel = Channel::Local(actor_ref);
    let channel_strategy: ChannelStrategy<u32> =
        ChannelStrategy::Forward(Forward::new(channel, NodeID::new(0), pool_info.clone()));

    fn map_fn_two(x: u32, state: &mut MapStateTwo<impl Backend>) -> ArconResult<u32> {
        state.rolling_counter().rmw(|v| {
            *v += 1;
        });
        Ok(x * 2)
    }

    // Set up Map Node

    let backend = Arc::new(Sled::create(test_dir_two).unwrap());
    let descriptor = String::from("map_state_two");
    let in_channels = vec![1.into()];
    let nm = NodeManager::new(descriptor.clone(), in_channels.clone(), backend.clone());
    let node_manager_comp_two = pipeline.system().create(|| nm);
    pipeline.connect_state_port(&node_manager_comp_two);

    pipeline
        .system()
        .start_notify(&node_manager_comp_two)
        .wait_timeout(std::time::Duration::from_millis(100))
        .expect("started");

    let node = Node::new(
        descriptor,
        channel_strategy,
        Map::stateful(MapStateTwo::new(backend.clone()), &map_fn_two),
        NodeState::new(NodeID::new(0), in_channels, backend),
    );

    let map_comp_two = pipeline.system().create(|| node);

    biconnect_components::<NodeManagerPort, _, _>(&node_manager_comp_two, &map_comp_two)
        .expect("connection");

    pipeline
        .system()
        .start_notify(&map_comp_two)
        .wait_timeout(std::time::Duration::from_millis(100))
        .expect("started");

    // create channel between maps
    let actor_ref: ActorRefStrong<ArconMessage<u32>> =
        map_comp_two.actor_ref().hold().expect("Failed to fetch");
    let channel = Channel::Local(actor_ref);
    let channel_strategy: ChannelStrategy<u32> =
        ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1), pool_info));

    // Set up Map Node

    let backend = Arc::new(Sled::create(test_dir).unwrap());
    let descriptor = String::from("map_state_one");
    let in_channels = vec![1.into(), 2.into(), 3.into()];

    let nm = NodeManager::new(descriptor.clone(), in_channels.clone(), backend.clone());
    let node_manager_comp = pipeline.system().create(|| nm);
    pipeline.connect_state_port(&node_manager_comp);

    pipeline
        .system()
        .start_notify(&node_manager_comp)
        .wait_timeout(std::time::Duration::from_millis(100))
        .expect("started");

    fn map_fn(x: u32, state: &mut MapStateOne<impl Backend>) -> ArconResult<u32> {
        state.events().append(x)?;
        Ok(x * 2)
    }

    let node = Node::new(
        descriptor,
        channel_strategy,
        Map::stateful(MapStateOne::new(backend.clone()), &map_fn),
        NodeState::new(NodeID::new(0), in_channels, backend),
    );

    let map_comp = pipeline.system().create(|| node);

    biconnect_components::<NodeManagerPort, _, _>(&node_manager_comp, &map_comp)
        .expect("connection");

    pipeline
        .system()
        .start_notify(&map_comp)
        .wait_timeout(std::time::Duration::from_millis(100))
        .expect("started");

    // Set up Batch component

    let batch_comp = pipeline.system().create(BatchComponent::<Sled, Sled>::new);

    pipeline
        .system()
        .start_notify(&batch_comp)
        .wait_timeout(std::time::Duration::from_millis(100))
        .expect("started");

    pipeline.watch(&["map_state_one", "map_state_two"], batch_comp);

    let node_ref = map_comp.actor_ref().hold().expect("fail");

    node_ref.tell(element(11, 1, 1));
    node_ref.tell(element(12, 1, 1));
    node_ref.tell(element(21, 1, 2));
    node_ref.tell(element(22, 1, 2));
    node_ref.tell(element(31, 1, 3));
    node_ref.tell(element(13, 1, 1));
    node_ref.tell(epoch(1, 1));
    node_ref.tell(epoch(1, 2));
    node_ref.tell(epoch(1, 3));

    node_ref.tell(element(100, 1, 1));
    node_ref.tell(element(101, 3, 1));
    node_ref.tell(element(102, 3, 1));
    wait(5);
}

fn element(data: u32, time: u64, sender: u32) -> ArconMessage<u32> {
    ArconMessage::element(data, Some(time), sender.into())
}

fn epoch(epoch: u64, sender: u32) -> ArconMessage<u32> {
    ArconMessage::epoch(epoch, sender.into())
}
fn wait(time: u64) {
    std::thread::sleep(std::time::Duration::from_secs(time));
}

#[derive(ComponentDefinition)]
pub struct BatchComponent<A, B>
where
    A: Backend,
    B: Backend,
{
    ctx: ComponentContext<Self>,
    curr_epoch: u64,
    snapshots: HashMap<StateID, SnapshotRef>,
}
impl<A, B> BatchComponent<A, B>
where
    A: Backend,
    B: Backend,
{
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            curr_epoch: 0,
            snapshots: HashMap::<StateID, SnapshotRef>::with_capacity(2),
        }
    }
    fn do_work(&self) {
        let s1 = self.snapshots.get("map_state_one").unwrap();
        let s2 = self.snapshots.get("map_state_two").unwrap();

        let s1_snapshot_dir = std::path::Path::new(&s1.snapshot.snapshot_path);
        let b1 = Arc::new(A::restore(&s1_snapshot_dir, &s1_snapshot_dir).unwrap());

        let s2_snapshot_dir = std::path::Path::new(&s2.snapshot.snapshot_path);
        let b2 = Arc::new(B::restore(&s2_snapshot_dir, &s2_snapshot_dir).unwrap());

        let mut query_state = QueryState::new(b1, b2);
        info!(
            self.ctx.log(),
            "Running through QueryState for epoch {}", self.curr_epoch
        );

        let events = query_state.events().consume().unwrap();

        for event in events {
            info!(self.ctx.log(), "Event {:?}", event);
        }

        info!(
            self.ctx.log(),
            "Rolling Count {:?}",
            query_state.rolling_counter().get()
        );
    }
}

impl<A, B> ComponentLifecycle for BatchComponent<A, B>
where
    A: Backend,
    B: Backend,
{
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }
}

impl<A, B> Actor for BatchComponent<A, B>
where
    A: Backend,
    B: Backend,
{
    type Message = SnapshotRef;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        info!(
            self.ctx.log(),
            "Got SnapShotRef For {} with Epoch {:?} and Path {:?}",
            msg.state_id,
            msg.snapshot.epoch,
            msg.snapshot.snapshot_path
        );

        if msg.snapshot.epoch >= self.curr_epoch {
            self.snapshots.insert(msg.state_id.clone(), msg);
            if self.snapshots.contains_key("map_state_one")
                && self.snapshots.contains_key("map_state_two")
            {
                self.do_work();
                self.snapshots.clear();
            }
        }
        Handled::Ok
    }

    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unreachable!();
    }
}
