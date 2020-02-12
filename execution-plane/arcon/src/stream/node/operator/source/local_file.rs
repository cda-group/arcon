// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::ArconType;
use crate::stream::node::source::SourceContext;
use kompact::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;

#[derive(ComponentDefinition)]
pub struct LocalFileSource<IN, OUT>
where
    IN: ArconType + FromStr,
    OUT: ArconType,
{
    ctx: ComponentContext<Self>,
    source_ctx: SourceContext<IN, OUT>,
    file_path: String,
}

impl<IN, OUT> LocalFileSource<IN, OUT>
where
    IN: ArconType + FromStr,
    OUT: ArconType,
{
    pub fn new(file_path: String, source_ctx: SourceContext<IN, OUT>) -> Self {
        LocalFileSource {
            ctx: ComponentContext::new(),
            source_ctx,
            file_path,
        }
    }
    pub fn process_file(&mut self) {
        if let Ok(f) = File::open(&self.file_path) {
            let reader = BufReader::new(f);
            let mut counter: u64 = 0;
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if let Ok(data) = l.parse::<IN>() {
                            let elem = self.source_ctx.extract_element(data);
                            let system = &self.ctx().system();
                            self.source_ctx.process(elem, system);
                            counter += 1;

                            if counter == self.source_ctx.watermark_interval {
                                self.source_ctx.generate_watermark(system);
                                counter = 0;
                            }
                        } else {
                            error!(self.ctx.log(), "Unable to parse line {}", l);
                        }
                    }
                    Err(e) => {
                        error!(
                            self.ctx.log(),
                            "Unable to read line with err {}",
                            e.to_string()
                        );
                    }
                }
            }
        } else {
            error!(self.ctx.log(), "Unable to open file {}", self.file_path);
        }
    }
}

impl<IN, OUT> Provide<ControlPort> for LocalFileSource<IN, OUT>
where
    IN: ArconType + FromStr,
    OUT: ArconType,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            self.process_file();
        }
    }
}

impl<IN, OUT> Actor for LocalFileSource<IN, OUT>
where
    IN: ArconType + FromStr,
    OUT: ArconType,
{
    type Message = ();
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{Channel, DebugNode, Forward, Map, NodeID};
    use kompact::default_components::DeadletterBox;
    use kompact::prelude::KompactSystem;
    use std::io::prelude::*;
    use std::sync::Arc;
    use std::{thread, time};
    use tempfile::NamedTempFile;

    // Shared methods for test cases
    fn wait(time: u64) {
        thread::sleep(time::Duration::from_secs(time));
    }

    fn test_setup<A: ArconType>() -> (KompactSystem, Arc<Component<DebugNode<A>>>) {
        // Kompact set-up
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = cfg.build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || {
            let s: DebugNode<A> = DebugNode::new();
            s
        });

        system.start(&sink);

        return (system, sink);
    }
    // Test cases
    #[test]
    fn local_file_u64_test() {
        let (system, sink) = test_setup::<u64>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        for _ in 0..50 {
            file.write_all(b"123\n").unwrap();
        }

        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy = Box::new(Forward::new(channel, NodeID::new(1)));

        // Our map function
        fn map_fn(x: u64) -> u64 {
            x + 5
        }

        let operator = Box::new(Map::<u64, u64>::new(&map_fn));

        // Set up SourceContext
        let buffer_limit = 200;
        let buffer_timeout = 0; // Not needed for LocalFileSource
        let watermark_interval = 25;

        let source_context = SourceContext::new(
            buffer_timeout,
            buffer_limit,
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            operator,
        );

        let file_source: LocalFileSource<u64, u64> =
            LocalFileSource::new(String::from(&file_path), source_context);
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(50 as usize));
        for item in &sink_inspect.data {
            // all elements should have been mapped + 5
            assert_eq!(item.data, 128 as u64);
        }
    }

    #[test]
    fn local_file_f64_test() {
        let (system, sink) = test_setup::<f64>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        let source_elements = 50;

        for i in 0..source_elements {
            let f = format!("{}.5\n", i);
            file.write_all(f.as_bytes()).unwrap();
        }

        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy = Box::new(Forward::new(channel, NodeID::new(1)));

        // just pass it on
        fn map_fn(x: f64) -> f64 {
            x
        }

        let operator = Box::new(Map::<f64, f64>::new(&map_fn));

        // Set up SourceContext
        let buffer_limit = 200;
        let buffer_timeout = 0; // Not needed for LocalFileSource
        let watermark_interval = 25;

        let source_context = SourceContext::new(
            buffer_timeout,
            buffer_limit,
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            operator,
        );

        let file_source: LocalFileSource<f64, f64> =
            LocalFileSource::new(String::from(&file_path), source_context);
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(source_elements as usize));
        for i in 0..source_elements {
            let expected: f64 = i as f64 + 0.5;
            assert_eq!(sink_inspect.data[i].data, expected);
        }
    }
}
