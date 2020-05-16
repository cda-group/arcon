// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{prelude::*, timer};
use once_cell::sync::Lazy;
use static_assertions::_core::sync::atomic::AtomicU64;
use std::{
    any::TypeId,
    cell::Cell,
    collections::HashMap,
    fs,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::{Mutex, RwLock},
};
use tempfile::NamedTempFile;

#[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Arcon, prost::Message, Clone, abomonation_derive::Abomonation)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct NormaliseElements {
    #[prost(int64, repeated, tag = "1")]
    pub data: Vec<i64>,
}

#[allow(dead_code)]
fn backend<SB: state::Backend>(
    state_dir: &Path,
    checkpoints_dir: &Path,
    node_id: u32,
) -> state::BackendContainer<SB> {
    let mut running_dir: PathBuf = state_dir.into();
    running_dir.push(format!("running_{}", node_id));
    let _ = fs::remove_dir_all(&running_dir); // ignore possible NotFound error
    fs::create_dir(&running_dir).unwrap();

    let dirs: Vec<String> = fs::read_dir(checkpoints_dir)
        .unwrap()
        .map(|d| d.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    let latest_complete_checkpoint = dirs
        .into_iter()
        .filter(|x| x.starts_with("checkpoint_"))
        .map(|x| x.split('_').last().unwrap().parse::<u64>().unwrap())
        .fold(HashMap::new(), |mut acc, el| {
            *acc.entry(el).or_insert_with(|| 0) += 1;
            acc
        })
        .into_iter()
        .filter(|e| e.1 == 3) // we have 3 nodes
        .map(|e| e.0)
        .max();

    match latest_complete_checkpoint {
        Some(epoch) => {
            let mut checkpoint_path: PathBuf = checkpoints_dir.into();
            checkpoint_path.push(format!(
                "checkpoint_{id}_{epoch}",
                id = node_id,
                epoch = epoch
            ));

            SB::restore(&running_dir, &checkpoint_path).unwrap()
        }
        None => SB::create(&running_dir).unwrap(),
    }
}

#[allow(dead_code)]
/// manually sent events -> Window -> Map -> LocalFileSink
fn run_pipeline<SB: state::Backend>(
    state_dir: &Path,
    sink_path: &Path,
    conf: ArconConf,
    crash_after: u32,
) -> Result<(), ()> {
    let timeout = std::time::Duration::from_millis(500);
    let mut pipeline = crate::pipeline::ArconPipeline::with_conf(conf);
    let pool_info = pipeline.get_pool_info();
    let checkpoint_dir = pipeline.arcon_conf().checkpoint_dir.clone();
    let _ = fs::create_dir(&checkpoint_dir);
    let system = &pipeline.system();

    // Create Sink Component
    let file_sink_node = system.create(|| {
        Node::new(
            "sink_node".into(),
            4.into(),
            vec![3.into()],
            ChannelStrategy::Mute,
            LocalFileSink::new(sink_path),
            backend::<SB>(state_dir, &checkpoint_dir, 4),
            timer::none(),
        )
    });
    system
        .start_notify(&file_sink_node)
        .wait_timeout(timeout)
        .expect("file sink node never started!");

    // Define Map
    let file_sink_ref: ActorRefStrong<ArconMessage<i64>> = file_sink_node
        .actor_ref()
        .hold()
        .expect("failed to fetch strong ref");
    let channel_strategy = ChannelStrategy::Forward(Forward::new(
        Channel::Local(file_sink_ref),
        NodeID::new(3),
        pool_info.clone(),
    ));

    let crash_after = Mutex::new(crash_after);
    let map_fn = move |x: NormaliseElements| -> i64 {
        let mut crash_after = crash_after.lock().unwrap();
        if *crash_after == 0 {
            panic!("expected panic!")
        }
        *crash_after -= 1;

        x.data.iter().map(|x| x + 3).sum()
    };

    let map_node = system.create(|| {
        Node::new(
            "map_node".into(),
            3.into(),
            vec![2.into()],
            channel_strategy,
            Map::new(map_fn),
            backend::<SB>(state_dir, &checkpoint_dir, 3),
            timer::none(),
        )
    });

    system
        .start_notify(&map_node)
        .wait_timeout(timeout)
        .expect("map node never started!");

    // Define Window
    fn window_fn(buffer: &[i64]) -> NormaliseElements {
        let sum: i64 = buffer.iter().sum();
        let count = buffer.len() as i64;
        let avg = sum / count;
        let data: Vec<i64> = buffer.iter().map(|x| x / avg).collect();
        NormaliseElements { data }
    }

    let mut window_state_backend = backend::<SB>(state_dir, &checkpoint_dir, 2);

    let window = AppenderWindow::new(&window_fn);

    let map_node_ref = map_node.actor_ref().hold().expect("Failed to fetch ref");
    let channel_strategy = ChannelStrategy::Forward(Forward::new(
        Channel::Local(map_node_ref),
        NodeID::new(2),
        pool_info,
    ));

    let window_node = system.create(move || {
        Node::new(
            "window_node".into(),
            2.into(),
            vec![1.into()],
            channel_strategy,
            EventTimeWindowAssigner::new(window, 2, 2, 0, false),
            window_state_backend,
            timer::wheel(),
        )
    });
    system
        .start_notify(&window_node)
        .wait_timeout(timeout)
        .expect("window never started!");

    // manually sending data, epochs and watermarks
    fn watermark(time: u64, sender: u32) -> ArconMessage<i64> {
        ArconMessage::watermark(time, sender.into())
    }

    fn element(data: i64, time: u64, sender: u32) -> ArconMessage<i64> {
        ArconMessage::element(data, Some(time), sender.into())
    }

    fn epoch(epoch: u64, sender: u32) -> ArconMessage<i64> {
        ArconMessage::epoch(epoch, sender.into())
    }

    let window_node_ref = window_node.actor_ref();
    window_node_ref.tell(element(1, 10, 1));
    window_node_ref.tell(element(2, 11, 1));
    window_node_ref.tell(element(3, 12, 1));

    window_node_ref.tell(watermark(11, 1));
    window_node_ref.tell(epoch(1, 1));

    window_node_ref.tell(element(4, 13, 1));
    window_node_ref.tell(element(42, 14, 1));

    window_node_ref.tell(watermark(14, 1));
    window_node_ref.tell(epoch(2, 1));

    window_node_ref.tell(element(69, 15, 1));

    window_node_ref.tell(watermark(20, 1));
    window_node_ref.tell(epoch(3, 1));

    std::thread::sleep(std::time::Duration::from_secs(1));

    pipeline.shutdown();

    // check if any of the nodes panicked
    if window_node.is_faulty() || map_node.is_faulty() || file_sink_node.is_faulty() {
        return Err(());
    }

    Ok(())
}

#[allow(dead_code)]
fn run_test<SB: state::Backend>() {
    let t = TypeId::of::<SB>();

    let test_dir = tempfile::tempdir().unwrap();
    let test_dir = test_dir.path();

    let mut conf = ArconConf::default();
    let mut checkpoint_dir: PathBuf = test_dir.into();
    checkpoint_dir.push("checkpoints");
    conf.checkpoint_dir = checkpoint_dir;

    // Define Sink File
    let sink_file = NamedTempFile::new().unwrap();
    let sink_path = sink_file.path();

    run_pipeline::<SB>(test_dir, sink_path, conf.clone(), 2).unwrap_err();
    {
        // Check results from the sink file!
        let file = File::open(sink_file.path()).expect("no such file");
        let buf = BufReader::new(file);
        let result: Vec<i64> = buf
            .lines()
            .map(|l| l.unwrap().parse::<i64>().expect("could not parse line"))
            .collect();

        assert_eq!(result, vec![9, 8]); // partial result after panic
    }

    run_pipeline::<SB>(test_dir, sink_path, conf, 100).unwrap();

    // Check results from the sink file!
    let buf = BufReader::new(sink_file);
    let result: Vec<i64> = buf
        .lines()
        .map(|l| l.unwrap().parse::<i64>().expect("could not parse line"))
        .collect();

    assert_eq!(result, vec![9, 8, 7]); // full result without repetitions
}

#[cfg(feature = "arcon_rocksdb")]
#[test]
fn test_rocks_recovery_pipeline() {
    run_test::<state::Rocks>()
}

#[cfg(all(feature = "arcon_sled", feature = "arcon_sled_checkpoints"))]
#[test]
fn test_sled_recovery_pipeline() {
    run_test::<state::Sled>()
}

#[cfg(all(feature = "arcon_faster", target_os = "linux"))]
#[test]
// flakey, sometimes passes, sometimes hangs, sometimes fails. But usually works as expected
// when debugging :(
#[ignore]
fn test_faster_recovery_pipeline() {
    run_test::<state::Faster>()
}
