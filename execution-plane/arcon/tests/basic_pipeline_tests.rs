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
    let cfg = KompactConfig::default();
    let system = KompactSystem::new(cfg).expect("Failed to create KompactSystem");

    // Set up Source File
    let mut source_file = NamedTempFile::new().unwrap();
    let source_path = source_file.path().to_string_lossy().into_owned();
    source_file.write_all(b"2\n4").unwrap();

    // Define Sink File
    let sink_file = NamedTempFile::new().unwrap();
    let sink_path = sink_file.path().to_string_lossy().into_owned();

    // Create Sink Component
    let node_5 = system.create_and_start(move || {
        let sink: LocalFileSink<i64> = LocalFileSink::new(&sink_path, vec!(4.to_string()));
        sink
    });

    // Create Map Task
    let channel = Channel::Local(node_5.actor_ref());
    let channel_strategy: Box<ChannelStrategy<i64>> = Box::new(Forward::new(channel));
    let code = String :: from ( "|x: vec[i64]| let m = merger[i64, +]; result(for(x, m, |b: merger[i64, +], i, e| merge(b, e + i64(3))))" ) ;
    let module = std::sync::Arc::new(Module::new(code).unwrap());
    let node_4 = system.create_and_start(move || {
        Node::<ArconVec<i64>, i64>::new(
            4.to_string(),
            vec!(3.to_string()),
            channel_strategy,
            Box::new(Map::<ArconVec<i64>, i64>::new(module))
        )
    });

    // Create Window Component
    let builder_code = String::from("||appender[i64]");
    let udf_code = String::from("|e:i64,w:appender[i64]| merge(w,e):appender[i64]");
    let materialiser_code = String::from("|e: appender[i64]| let elem = result(e); let sum = result(for(elem, merger[i64, +], |b: merger[i64, +], i: i64, e: i64| merge(b, e))); 
                                         let count = len(elem); let avg = sum / count; result(for(elem, appender[i64], |b: appender[i64], i: i64, e: i64| merge(b, e / avg)))") ;

    let channel_strategy: Box<Forward<ArconVec<i64>>> =
        Box::new(Forward::new(Channel::Local(node_4.actor_ref())));

    let node_3 = system.create_and_start(move || {
        Node::<i64, ArconVec<i64>>::new(
            3.to_string(),
            vec!(2.to_string()),
            channel_strategy,
            Box::new(EventTimeWindowAssigner::<i64, Appender<i64>, ArconVec<i64>>::new(
                builder_code,
                udf_code,
                materialiser_code,
                3,
                3,
                0,
                false,
            ))
        )
    });

    // Create Filter Task
    let channel = Channel::Local(node_3.actor_ref());
    let channel_strategy: Box<ChannelStrategy<i64>> = Box::new(Forward::new(channel));
    let code = String::from("|x: i64| x < i64(5)");
    let module = std::sync::Arc::new(Module::new(code).unwrap());
    let node_2 = system.create_and_start(move || {
        Node::<i64, i64>::new(
            2.to_string(),
            vec!(1.to_string()),
            channel_strategy,
            Box::new(Filter::<i64>::new(module))
        )
    });

    // Define Source
    let channel = Channel::Local(node_2.actor_ref());
    let channel_strategy: Box<ChannelStrategy<i64>> = Box::new(Forward::new(channel));

    // Watermark per 5 lines in the file 
    let wm_interval = 5;
    let _ = system.create_and_start(move || {
        let source: LocalFileSource<i64> =
            LocalFileSource::new(String::from(&source_path), channel_strategy, wm_interval, 1.to_string());
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
