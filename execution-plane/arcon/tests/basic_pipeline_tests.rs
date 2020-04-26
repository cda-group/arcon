// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! The following tests will look similar to the generated code from `arcon_codegen`.
//! The purpose of these tests are to verify the results of end-to-end pipelines.

#![allow(bare_trait_objects)]
extern crate arcon;

use arcon::{macros::*, prelude::*, timer};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};
use tempfile::NamedTempFile;

#[arcon]
pub struct NormaliseElements {
    #[prost(int64, repeated, tag = "1")]
    pub data: Vec<i64>,
}

#[arcon]
pub struct SourceData {
    #[prost(int64, tag = "1")]
    pub data: i64,
    #[prost(uint64, tag = "2")]
    pub timestamp: u64,
}

/// `normalise_pipeline_test`
/// CollectionSource -> Window -> Map -> LocalFileSink
#[test]
fn normalise_pipeline_test() {
    // TODO: It can use some more love....

    let timeout = std::time::Duration::from_millis(500);
    let mut pipeline = ArconPipeline::new();
    let pool_info = pipeline.get_pool_info();
    let system = pipeline.system();

    // Define Sink File
    let sink_file = NamedTempFile::new().unwrap();
    let sink_path = sink_file.path().to_string_lossy().into_owned();

    // Create Sink Component
    let node_4 = system.create(move || {
        Node::new(
            String::from("sink_node"),
            4.into(),
            vec![3.into()],
            ChannelStrategy::Mute,
            LocalFileSink::new(&sink_path),
            Box::new(InMemory::new("test5".as_ref()).unwrap()),
            timer::none,
        )
    });
    system
        .start_notify(&node_4)
        .wait_timeout(timeout)
        .expect("node_4 never started!");

    // Define Map
    let actor_ref: ActorRefStrong<ArconMessage<i64>> = node_4
        .actor_ref()
        .hold()
        .expect("failed to fetch strong ref");
    let channel = Channel::Local(actor_ref);
    let channel_strategy =
        ChannelStrategy::Forward(Forward::new(channel, NodeID::new(3), pool_info.clone()));

    fn map_fn(x: NormaliseElements) -> i64 {
        x.data.iter().map(|x| x + 3).sum()
    }

    let node_3 = system.create(move || {
        Node::new(
            String::from("map_node"),
            3.into(),
            vec![2.into()],
            channel_strategy,
            Map::<NormaliseElements, i64>::new(&map_fn),
            Box::new(InMemory::new("test4".as_ref()).unwrap()),
            timer::none,
        )
    });

    system
        .start_notify(&node_3)
        .wait_timeout(timeout)
        .expect("node_3 never started!");

    // Define Window

    fn window_fn(buffer: &[i64]) -> NormaliseElements {
        let sum: i64 = buffer.iter().sum();
        let count = buffer.len() as i64;
        let avg = sum / count;
        let data: Vec<i64> = buffer.iter().map(|x| x / avg).collect();
        NormaliseElements { data }
    }

    let mut state_backend_2 = Box::new(InMemory::new("test2".as_ref()).unwrap());

    let window: Box<dyn Window<i64, NormaliseElements>> =
        Box::new(AppenderWindow::new(&window_fn, &mut *state_backend_2));

    let node_3_actor_ref = node_3.actor_ref().hold().expect("Failed to fetch ref");
    let channel_strategy = ChannelStrategy::Forward(Forward::new(
        Channel::Local(node_3_actor_ref),
        NodeID::new(2),
        pool_info.clone(),
    ));

    let node_2 = system.create(move || {
        Node::new(
            String::from("window_node"),
            2.into(),
            vec![1.into()],
            channel_strategy,
            EventTimeWindowAssigner::<i64, NormaliseElements>::new(
                window,
                2,
                2,
                0,
                false,
                &mut *state_backend_2,
            ),
            state_backend_2,
            timer::wheel,
        )
    });
    system
        .start_notify(&node_2)
        .wait_timeout(timeout)
        .expect("node_2 never started!");

    // Define Source
    fn source_map(x: SourceData) -> i64 {
        x.data
    }

    let actor_ref: ActorRefStrong<ArconMessage<i64>> = node_2
        .actor_ref()
        .hold()
        .expect("Failed to fetch strong ref");
    let channel = Channel::Local(actor_ref);
    let channel_strategy =
        ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1), pool_info.clone()));

    let watermark_interval = 2;

    fn timestamp_extractor(x: &SourceData) -> u64 {
        x.timestamp
    }

    let source_context = SourceContext::new(
        watermark_interval,
        Some(&timestamp_extractor),
        channel_strategy,
        Map::<SourceData, i64>::new(&source_map),
        Box::new(InMemory::new("test".as_ref()).unwrap()),
        timer::none,
    );

    let mut collection: Vec<SourceData> = Vec::new();
    collection.push(SourceData {
        data: 2,
        timestamp: 1,
    });
    collection.push(SourceData {
        data: 4,
        timestamp: 3,
    });

    let node_1 = system.create(move || {
        let collection_source = CollectionSource::new(collection, source_context);
        collection_source
    });

    system
        .start_notify(&node_1)
        .wait_timeout(timeout)
        .expect("node_1 never started!");

    std::thread::sleep(std::time::Duration::from_secs(1));

    // Only a single window should have been triggered.
    // Check results from the sink file!
    let file = File::open(sink_file.path()).expect("no such file");
    let buf = BufReader::new(file);
    let result: Vec<i64> = buf
        .lines()
        .map(|l| l.unwrap().parse::<i64>().expect("could not parse line"))
        .collect();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], 4);
    pipeline.shutdown();
}
