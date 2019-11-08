//! The following tests will look similar to the generated code from `arcon_codegen`.
//! The purpose of these tests are to verify the results of end-to-end pipelines.

#![allow(bare_trait_objects)]
extern crate arcon;

use arcon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::io::{BufRead, BufReader};
use tempfile::NamedTempFile;

/// `normalise_pipeline_test`
/// LocalFileSource -> Filter -> Window -> Map -> LocalFileSink
#[test]
fn normalise_pipeline_test() {
    let system = KompactConfig::default().build().expect("KompactSystem");

    // Set up Source File
    let mut source_file = NamedTempFile::new().unwrap();
    let source_path = source_file.path().to_string_lossy().into_owned();
    source_file.write_all(b"2\n4").unwrap();

    // Define Sink File
    let sink_file = NamedTempFile::new().unwrap();
    let sink_path = sink_file.path().to_string_lossy().into_owned();

    // Create Sink Component
    let node_5 = system.create_and_start(move || {
        let sink: LocalFileSink<i64> = LocalFileSink::new(&sink_path, vec![4.into()]);
        sink
    });

    // Define Map
    let actor_ref: ActorRefStrong<ArconMessage<i64>> = node_5
        .actor_ref()
        .hold()
        .expect("failed to fetch strong ref");
    let channel = Channel::Local(actor_ref);
    let channel_strategy: Box<ChannelStrategy<i64>> = Box::new(Forward::new(channel));

    fn map_fn(x: Vec<i64>) -> i64 {
        x.iter().map(|x| x + 3).sum()
    }

    let node_4 = system.create_and_start(move || {
        Node::<Vec<i64>, i64>::new(
            4.into(),
            vec![3.into()],
            channel_strategy,
            Box::new(Map::<Vec<i64>, i64>::new(&map_fn)),
        )
    });

    // Define Window

    fn window_fn(buffer: &Vec<i64>) -> Vec<i64> {
        let sum: i64 = buffer.iter().sum();
        let count = buffer.len() as i64;
        let avg = sum / count;
        buffer.iter().map(|x| x / avg).collect()
    }

    let window: Box<Window<i64, Vec<i64>>> = Box::new(AppenderWindow::new(&window_fn));

    let node_4_actor_ref = node_4.actor_ref().hold().expect("Failed to fetch ref");
    let channel_strategy: Box<Forward<Vec<i64>>> =
        Box::new(Forward::new(Channel::Local(node_4_actor_ref)));

    let node_3 = system.create_and_start(move || {
        Node::<i64, Vec<i64>>::new(
            3.into(),
            vec![2.into()],
            channel_strategy,
            Box::new(EventTimeWindowAssigner::<i64, Vec<i64>>::new(
                window, 3, 3, 0, false,
            )),
        )
    });

    // Define Filter

    let node_3_actor_ref = node_3.actor_ref().hold().expect("Failed to fetch ref");
    let channel = Channel::Local(node_3_actor_ref);
    let channel_strategy: Box<ChannelStrategy<i64>> = Box::new(Forward::new(channel));
    fn filter_fn(x: &i64) -> bool {
        *x < 5
    }
    let node_2 = system.create_and_start(move || {
        Node::<i64, i64>::new(
            2.into(),
            vec![1.into()],
            channel_strategy,
            Box::new(Filter::<i64>::new(&filter_fn)),
        )
    });

    // Define Source
    let actor_ref: ActorRefStrong<ArconMessage<i64>> = node_2
        .actor_ref()
        .hold()
        .expect("Failed to fetch strong ref");
    let channel = Channel::Local(actor_ref);
    let channel_strategy: Box<ChannelStrategy<i64>> = Box::new(Forward::new(channel));

    // Watermark per 5 lines in the file
    let wm_interval = 5;
    let _ = system.create_and_start(move || {
        let source: LocalFileSource<i64> = LocalFileSource::new(
            String::from(&source_path),
            channel_strategy,
            wm_interval,
            1.into(),
        );
        source
    });

    std::thread::sleep(std::time::Duration::from_secs(5));

    // Only a single window should have been triggered.
    // Check results from the sink file!
    let file = File::open(sink_file.path()).expect("no such file");
    let buf = BufReader::new(file);
    let result: Vec<i64> = buf
        .lines()
        .map(|l| l.unwrap().parse::<i64>().expect("could not parse line"))
        .collect();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], 7);
    let _ = system.shutdown();
}
