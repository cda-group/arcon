// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::ArconNever,
    prelude::state,
    stream::{operator::Operator, source::SourceContext},
    timer::TimerBackend,
};
use kompact::prelude::*;
use std::{
    cell::RefCell,
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

#[derive(ComponentDefinition)]
pub struct LocalFileSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    OP::IN: FromStr,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    ctx: ComponentContext<Self>,
    source_ctx: RefCell<SourceContext<OP, B, T>>,
    file_path: String,
}

impl<OP, B, T> LocalFileSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    OP::IN: FromStr,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    pub fn new(file_path: String, source_ctx: SourceContext<OP, B, T>) -> Self {
        LocalFileSource {
            ctx: ComponentContext::new(),
            source_ctx: RefCell::new(source_ctx),
            file_path,
        }
    }
    pub fn process_file(&mut self) {
        if let Ok(f) = File::open(&self.file_path) {
            let reader = BufReader::new(f);
            let mut counter: u64 = 0;
            let mut source_ctx = self.source_ctx.borrow_mut();
            let interval = source_ctx.watermark_interval;
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if let Ok(data) = l.parse::<OP::IN>() {
                            let elem = source_ctx.extract_element(data);
                            source_ctx.process(elem, self);
                            counter += 1;

                            if counter == interval {
                                source_ctx.generate_watermark(self);
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
            // We are done, generate a watermark...
            source_ctx.generate_watermark(self);
        } else {
            error!(self.ctx.log(), "Unable to open file {}", self.file_path);
        }
    }
}

impl<OP, B, T> Provide<ControlPort> for LocalFileSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    OP::IN: FromStr,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            self.process_file();
        }
    }
}

impl<OP, B, T> NetworkActor for LocalFileSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    OP::IN: FromStr,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    type Message = Never;
    type Deserialiser = Never;

    fn receive(&mut self, _sender: Option<ActorPath>, _msg: Self::Message) {
        unreachable!(ArconNever::IS_UNREACHABLE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::{ArconF64, ArconType},
        pipeline::ArconPipeline,
        prelude::{Channel, ChannelStrategy, DebugNode, Forward, Map, NodeID},
        state::{Backend, InMemory},
        timer,
    };
    use std::{io::prelude::*, sync::Arc, thread, time};
    use tempfile::NamedTempFile;

    // Shared methods for test cases
    fn wait(time: u64) {
        thread::sleep(time::Duration::from_secs(time));
    }

    fn test_setup<A: ArconType>() -> (ArconPipeline, Arc<Component<DebugNode<A>>>) {
        let mut pipeline = ArconPipeline::new();
        let system = pipeline.system();
        let (sink, _) = system.create_and_register(move || {
            let s: DebugNode<A> = DebugNode::new();
            s
        });

        system.start(&sink);

        return (pipeline, sink);
    }
    // Test cases
    #[test]
    fn local_file_u64_test() {
        let (mut pipeline, sink) = test_setup::<u64>();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        for _ in 0..50 {
            file.write_all(b"123\n").unwrap();
        }

        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1), pool_info));

        // Our map function
        fn map_fn(x: u64) -> u64 {
            x + 5
        }

        // Set up SourceContext
        let watermark_interval = 25;

        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Map::new(&map_fn),
            InMemory::create("test".as_ref()).unwrap(),
            timer::none(),
        );

        let file_source = LocalFileSource::new(String::from(&file_path), source_context);
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(50 as usize));
        for item in &sink_inspect.data {
            // all elements should have been mapped + 5
            assert_eq!(item.data, 128);
        }
    }

    #[test]
    fn local_file_f64_test() {
        let (mut pipeline, sink) = test_setup::<ArconF64>();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        let source_elements = 50;

        for i in 0..source_elements {
            let f = format!("{}.5\n", i);
            file.write_all(f.as_bytes()).unwrap();
        }

        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1), pool_info));

        // just pass it on
        fn map_fn(x: ArconF64) -> ArconF64 {
            x
        }

        // Set up SourceContext
        let watermark_interval = 25;

        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Map::new(&map_fn),
            InMemory::create("test".as_ref()).unwrap(),
            timer::none(),
        );

        let file_source = LocalFileSource::new(String::from(&file_path), source_context);
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(source_elements as usize));
        for i in 0..source_elements {
            let expected: ArconF64 = ArconF64::new(i as f64 + 0.5);
            assert_eq!(sink_inspect.data[i].data, expected);
        }
    }
}
