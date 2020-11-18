// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::ArconNever,
    prelude::state,
    stream::{operator::Operator, source::SourceContext},
};
use kompact::prelude::*;
use std::{
    cell::RefCell,
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

#[derive(ComponentDefinition)]
pub struct LocalFileSource<OP, B>
where
    OP: Operator + 'static,
    OP::IN: FromStr,
    B: state::Backend,
{
    ctx: ComponentContext<Self>,
    source_ctx: RefCell<SourceContext<OP, B>>,
    file_path: String,
}

impl<OP, B> LocalFileSource<OP, B>
where
    OP: Operator + 'static,
    OP::IN: FromStr,
    B: state::Backend,
{
    pub fn new(file_path: String, source_ctx: SourceContext<OP, B>) -> Self {
        LocalFileSource {
            ctx: ComponentContext::uninitialised(),
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
                            if let Err(err) = source_ctx.process(elem, self) {
                                error!(self.ctx.log(), "Error while processing record {:?}", err);
                            }
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

impl<OP, B> ComponentLifecycle for LocalFileSource<OP, B>
where
    OP: Operator + 'static,
    OP::IN: FromStr,
    B: state::Backend,
{
    fn on_start(&mut self) -> Handled {
        self.process_file();
        Handled::Ok
    }
}

impl<OP, B> NetworkActor for LocalFileSource<OP, B>
where
    OP: Operator + 'static,
    OP::IN: FromStr,
    B: state::Backend,
{
    type Message = Never;
    type Deserialiser = Never;

    fn receive(&mut self, _sender: Option<ActorPath>, _msg: Self::Message) -> Handled {
        unreachable!(ArconNever::IS_UNREACHABLE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::{ArconF64, ArconType},
        pipeline::Pipeline,
        prelude::{Channel, ChannelStrategy, DebugNode, Forward, Map, NodeID},
    };
    use arcon_error::ArconResult;
    use std::{io::prelude::*, sync::Arc, thread, time};
    use tempfile::NamedTempFile;

    // Shared methods for test cases
    fn wait(time: u64) {
        thread::sleep(time::Duration::from_secs(time));
    }

    fn test_setup<A: ArconType>() -> (Pipeline, Arc<Component<DebugNode<A>>>) {
        let mut pipeline = Pipeline::new();
        let system = pipeline.system();
        let sink = system.create(move || {
            let s: DebugNode<A> = DebugNode::new();
            s
        });

        system
            .start_notify(&sink)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        (pipeline, sink)
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
        fn map_fn(x: ArconF64) -> ArconResult<ArconF64> {
            Ok(x)
        }

        // Set up SourceContext
        let watermark_interval = 25;
        let backend = std::sync::Arc::new(crate::util::temp_backend());

        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Map::new(&map_fn),
            backend,
        );

        let source_comp =
            system.create(|| LocalFileSource::new(String::from(&file_path), source_context));

        system
            .start_notify(&source_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        wait(1);

        sink.on_definition(|cd| {
            assert_eq!(&cd.data.len(), &(source_elements as usize));
            for i in 0..source_elements {
                let expected: ArconF64 = ArconF64::new(i as f64 + 0.5);
                assert_eq!(cd.data[i].data, expected);
            }
        });
    }
}
