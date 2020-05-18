// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
// A simple pipeline to profile arcon.
// Can be used to identify performance regressions..
use arcon::{
    prelude::*,
    state::{metered::Metrics, Faster, InMemory, Metered, Rocks, Sled},
    timer,
};
use experiments::{
    get_items, square_root_newton,
    throughput_sink::{Run, ThroughputSink},
    EnrichedItem, Item,
};
use std::{fs, sync::Arc};
use structopt::{clap::arg_enum, StructOpt};

arg_enum! {
    #[derive(Clone, Debug)]
    enum StateBackendType {
        InMemory,
        Rocks,
        Sled,
        Faster,
        MeteredInMemory,
        MeteredRocks,
        MeteredSled,
        MeteredFaster,
    }
}

#[derive(StructOpt, Debug, Clone)]
struct Opts {
    /// Number of threads for KompactSystem
    #[structopt(short = "t", long, default_value = "4")]
    kompact_threads: usize,
    /// Batch size for ChannelStrategy
    #[structopt(short = "b", long, default_value = "1024")]
    batch_size: u64,
    /// Amount of items for the collection source
    #[structopt(short = "z", long, default_value = "10000000")]
    collection_size: usize,
    /// Kompact cfg throughput
    #[structopt(short = "k", long, default_value = "50")]
    kompact_throughput: u64,
    /// How often we log throughput
    #[structopt(short = "f", long, default_value = "100000")]
    log_frequency: u64,
    /// workload scaling
    #[structopt(short = "s", long, default_value = "1")]
    scaling_factor: u64,
    /// dedicated mode
    #[structopt(short = "d", long)]
    dedicated: bool,
    /// dedicated-pinned mode
    #[structopt(short = "p", long = "dedicated-pinned")]
    pinned: bool,
    /// dedicated-pinned mode
    #[structopt(short = "l", long)]
    log_throughput: bool,
    /// state backend type
    #[structopt(
        long,
        possible_values = &StateBackendType::variants(),
        case_insensitive = true,
        default_value = "InMemory"
    )]
    state_backend_type: StateBackendType,
}

fn main() {
    let Opts {
        kompact_threads,
        batch_size,
        collection_size,
        kompact_throughput,
        log_frequency,
        scaling_factor,
        dedicated,
        pinned,
        log_throughput,
        state_backend_type,
    } = Opts::from_args();

    macro_rules! exec {
        ($SB:ty) => {
            exec::<$SB>(
                scaling_factor,
                collection_size,
                batch_size,
                kompact_threads,
                log_frequency,
                kompact_throughput,
                dedicated,
                pinned,
                log_throughput,
            )
        };
    }

    match state_backend_type {
        StateBackendType::InMemory => exec!(InMemory),
        StateBackendType::Rocks => exec!(Rocks),
        StateBackendType::Sled => exec!(Sled),
        StateBackendType::Faster => exec!(Faster),
        StateBackendType::MeteredInMemory => exec!(Metered<InMemory>),
        StateBackendType::MeteredRocks => exec!(Metered<Rocks>),
        StateBackendType::MeteredSled => exec!(Metered<Sled>),
        StateBackendType::MeteredFaster => exec!(Metered<Faster>),
    }
}

// CollectionSource -> Map Node -> ThroughputSink
fn exec<SB: state::Backend>(
    scaling_factor: u64,
    collection_size: usize,
    batch_size: u64,
    kompact_threads: usize,
    log_freq: u64,
    kompact_throughput: u64,
    dedicated: bool,
    pinned: bool,
    log_throughput: bool,
) {
    let arcon_config = ArconConf::default(); // TODO: make an actual pipeline out of this
    let _ = fs::remove_dir_all(&arcon_config.checkpoint_dir);
    fs::create_dir_all(&arcon_config.checkpoint_dir).unwrap();
    let _ = fs::remove_dir_all(&arcon_config.state_dir);
    fs::create_dir_all(&arcon_config.state_dir).unwrap();

    let core_ids = get_core_ids().unwrap();
    // So we don't pin on cores that the Kompact workers also pinned to.
    let mut core_counter: usize = kompact_threads;
    let timeout = std::time::Duration::from_millis(500);

    let mut conf = ArconConf::default();
    conf.kompact_threads = kompact_threads;
    conf.kompact_throughput = kompact_throughput as usize;

    let mut pipeline = ArconPipeline::with_conf(conf);
    let pool_info = pipeline.get_pool_info();
    let system = pipeline.system();

    let total_items = collection_size.next_power_of_two();

    let sink = system.create(move || {
        ThroughputSink::<experiments::EnrichedItem>::new(
            log_freq,
            log_throughput,
            total_items as u64,
        )
    });
    let sink_port = sink.on_definition(|cd| cd.sink_port.share());

    system
        .start_notify(&sink)
        .wait_timeout(timeout)
        .expect("sink never started!");

    let sink_ref: ActorRefStrong<ArconMessage<EnrichedItem>> = sink.actor_ref().hold().expect("no");
    let sink_channel = Channel::Local(sink_ref);

    // Map comp...
    #[inline(always)]
    fn map_fn(item: Item) -> EnrichedItem {
        // Workload function that we can adjust with the scaling factor
        let root = square_root_newton(item.number, item.scaling_factor as usize);

        EnrichedItem {
            id: item.id,
            root: Some(root),
        }
    }

    let channel_strategy = ChannelStrategy::Forward(Forward::new(
        sink_channel,
        NodeID::new(1),
        pool_info.clone(),
    ));

    let sb_config = state::Config {
        live_state_base_path: arcon_config.state_dir.clone(),
        checkpoints_base_path: arcon_config.checkpoint_dir.clone(),
        backend_ids: vec![1.to_string(), 2.to_string()],
    };

    let node = Node::new(
        String::from("map_node"),
        1.into(),
        vec![2.into()],
        channel_strategy,
        Map::new(&map_fn),
        SB::restore_or_create(&sb_config, 1.to_string()).unwrap(),
        timer::none(),
    );

    let map_node = if dedicated {
        if pinned {
            assert!(core_counter < core_ids.len());
            core_counter += 1;
            println!(
                "Starting Map node using pinned component on core id {}",
                core_counter - 1
            );
            system.create_dedicated_pinned(move || node, core_ids[core_counter - 1])
        } else {
            system.create_dedicated(move || node)
        }
    } else {
        system.create(move || node)
    };

    system
        .start_notify(&map_node)
        .wait_timeout(timeout)
        .expect("map_node never started!");

    // Set up Source

    // Just an identity function
    fn mapper(item: Item) -> Item {
        item
    }
    let watermark_interval = batch_size * 4;

    // Set up channel for source to Map node
    let node_ref: ActorRefStrong<ArconMessage<Item>> = map_node.actor_ref().hold().expect("no");
    let node_channel = Channel::Local(node_ref);
    let channel_strategy =
        ChannelStrategy::Forward(Forward::new(node_channel, NodeID::new(2), pool_info));

    let source_context = SourceContext::new(
        watermark_interval,
        None, // no timestamp extractor
        channel_strategy,
        Map::new(&mapper),
        SB::restore_or_create(&sb_config, 2.to_string()).unwrap(),
        timer::none(),
    );

    // Collection for source
    let items = get_items(scaling_factor, total_items as usize);
    println!("Finished generating items...");

    // Set up CollectionSource component
    let collection_source = CollectionSource::new(items, source_context);

    let source = {
        if pinned {
            assert!(core_counter < core_ids.len());
            core_counter += 1;
            println!(
                "Starting source using pinned component on core id {}",
                core_counter - 1
            );
            system.create_dedicated_pinned(move || collection_source, core_ids[core_counter - 1])
        } else {
            system.create_dedicated(move || collection_source)
        }
    };

    system
        .start_notify(&source)
        .wait_timeout(timeout)
        .expect("source never started!");

    // Set up start time at Sink
    let (promise, future) = kpromise();
    system.trigger_r(Run::new(promise), &sink_port);

    // wait for sink to return completion msg.
    let res = future.wait();
    println!("=== Execution took {:?} milliseconds ===", res.as_millis());
    print_state_backend_metrics(source, map_node);

    pipeline.shutdown();
}

trait HasMetrics: ComponentDefinition + ActorRaw + Sized + 'static {
    fn get_metrics(&mut self) -> Option<&mut Metrics>;
    fn backend_name(&self) -> &'static str;
}

impl<OP, B, T> HasMetrics for Node<OP, B, T>
where
    OP: Operator<B>,
    B: state::Backend,
    T: timer::TimerBackend<OP::TimerState>,
{
    fn get_metrics(&mut self) -> Option<&mut Metrics> {
        self.state_backend.inner.get_mut().metrics()
    }

    fn backend_name(&self) -> &'static str {
        std::any::type_name::<B>()
    }
}

impl<OP, B, T> HasMetrics for CollectionSource<OP, B, T>
where
    OP: Operator<B>,
    B: state::Backend,
    T: timer::TimerBackend<OP::TimerState>,
{
    fn get_metrics(&mut self) -> Option<&mut Metrics> {
        self.source_ctx.state_backend.get_mut().metrics()
    }

    fn backend_name(&self) -> &'static str {
        std::any::type_name::<B>()
    }
}

fn print_state_backend_metrics(
    source: Arc<Component<impl HasMetrics>>,
    map_node: Arc<Component<impl HasMetrics>>,
) {
    let mut source_def = source.definition().lock().unwrap();
    let mut map_def = map_node.definition().lock().unwrap();
    let name = source_def.backend_name();

    let (source_metrics, map_metrics) = match (source_def.get_metrics(), map_def.get_metrics()) {
        (Some(s), Some(m)) => (s, m),
        _ => return,
    };

    println!("State backend metrics for `{}`", name);
    println!("\nmin, avg, and max are measured in nanoseconds\n\nSource node metrics");
    println!("{}\n", source_metrics.summary());
    println!("Map node metrics");
    println!("{}", map_metrics.summary());
}
