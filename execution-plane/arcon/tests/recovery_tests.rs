// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// similar to basic_pipeline_tests, but with changes to test recovery capabilities

#![allow(bare_trait_objects)]
extern crate arcon;

use arcon::{macros::*, prelude::*};
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    fs,
    fs::File,
    io::{BufRead, BufReader},
    sync::RwLock,
};
use tempfile::NamedTempFile;

#[arcon]
pub struct NormaliseElements {
    #[prost(int64, repeated, tag = "1")]
    pub data: Vec<i64>,
}

static PANIC_COUNTDOWN: Lazy<RwLock<u32>> = Lazy::new(|| RwLock::new(0));

fn rocks_backend(node_id: u32) -> Box<dyn StateBackend> {
    let runtime_dir = format!("running_{}", node_id);
    let _ = fs::remove_dir_all(&runtime_dir);
    fs::create_dir(&runtime_dir).unwrap();

    let dirs: Vec<String> = fs::read_dir(std::env::current_dir().unwrap())
        .unwrap()
        .map(|d| d.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    let latest_complete_checkpoint: Option<u32> = dirs
        .into_iter()
        .filter(|x| x.starts_with("checkpoint_"))
        .map(|x| (x.chars().last().unwrap().to_digit(10).unwrap(), x))
        .fold(HashMap::new(), |mut acc, el| {
            acc.entry(el.0)
                .or_insert_with(|| Vec::with_capacity(3))
                .push(el.1);
            acc
        })
        .into_iter()
        .filter(|e| e.1.len() == 3) // we have 3 nodes
        .map(|e| e.0)
        .max();

    match latest_complete_checkpoint {
        Some(epoch) => Box::new(
            RocksDb::restore(
                &format!("checkpoint_{id}_{epoch}", id = node_id, epoch = epoch),
                &runtime_dir,
            )
            .unwrap(),
        ),
        None => Box::new(RocksDb::new(&runtime_dir).unwrap()),
    }
}

/// CollectionSource -> Window -> Map -> LocalFileSink
fn run_pipeline(sink_path: &str) -> Result<(), ()> {
    let timeout = std::time::Duration::from_millis(500);
    let system = KompactConfig::default().build().expect("KompactSystem");

    // Create Sink Component
    let file_sink_node = system.create(move || {
        Node::new(
            4.into(),
            vec![3.into()],
            ChannelStrategy::Mute,
            Box::new(LocalFileSink::new(sink_path)),
            rocks_backend(4),
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
    let channel_strategy =
        ChannelStrategy::Forward(Forward::new(Channel::Local(file_sink_ref), NodeID::new(3)));

    fn map_fn(x: NormaliseElements) -> i64 {
        if *PANIC_COUNTDOWN.read().unwrap() == 0 {
            panic!("AAAAAA!!!")
        }
        *PANIC_COUNTDOWN.write().unwrap() -= 1;

        x.data.iter().map(|x| x + 3).sum()
    }

    let map_node = system.create(move || {
        Node::<NormaliseElements, i64>::new(
            3.into(),
            vec![2.into()],
            channel_strategy,
            Box::new(Map::<NormaliseElements, i64>::new(&map_fn)),
            rocks_backend(3),
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

    let mut window_state_backend = rocks_backend(2);

    let window: Box<dyn Window<i64, NormaliseElements>> =
        Box::new(AppenderWindow::new(&window_fn, &mut *window_state_backend));

    let map_node_ref = map_node.actor_ref().hold().expect("Failed to fetch ref");
    let channel_strategy =
        ChannelStrategy::Forward(Forward::new(Channel::Local(map_node_ref), NodeID::new(2)));

    let window_node = system.create(move || {
        Node::<i64, NormaliseElements>::new(
            2.into(),
            vec![1.into()],
            channel_strategy,
            Box::new(EventTimeWindowAssigner::<i64, NormaliseElements>::new(
                window,
                2,
                2,
                0,
                false,
                &mut *window_state_backend,
            )),
            window_state_backend,
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

    system.shutdown().expect("Shutdown failed");

    // check if any of the nodes panicked
    if window_node.is_faulty() || map_node.is_faulty() || file_sink_node.is_faulty() {
        return Err(());
    }

    Ok(())
}

#[test]
fn run_test() {
    let test_directory = "./recovery_test";
    let _ = fs::remove_dir_all(test_directory);
    fs::create_dir(test_directory).unwrap();

    std::env::set_current_dir(test_directory).unwrap();

    // Define Sink File
    let sink_file = NamedTempFile::new().unwrap();
    let sink_path = sink_file.path().to_string_lossy().into_owned();

    *PANIC_COUNTDOWN.write().unwrap() = 2; // reaches zero quite quickly
    run_pipeline(&sink_path).unwrap_err();
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

    *PANIC_COUNTDOWN.write().unwrap() = 100; // won't reach zero
    run_pipeline(&sink_path).unwrap();

    // Check results from the sink file!
    let file = File::open(sink_file.path()).expect("no such file");
    let buf = BufReader::new(file);
    let result: Vec<i64> = buf
        .lines()
        .map(|l| l.unwrap().parse::<i64>().expect("could not parse line"))
        .collect();

    assert_eq!(result, vec![9, 8, 7]); // full result without repetitions
}
