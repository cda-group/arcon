// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::ArconType,
    prelude::state,
    stream::{
        operator::Operator,
        source::{Source, SourceContext},
    },
};
use kompact::prelude::*;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    marker::PhantomData,
    str::FromStr,
};

const RESCHEDULE_EVERY: usize = 10000;

pub struct LocalFileSource<A: ArconType + FromStr> {
    lines: Vec<String>,
    _marker: PhantomData<A>,
}

impl<A> LocalFileSource<A>
where
    A: ArconType + FromStr,
{
    pub fn new(file_path: String) -> Self {
        let f = File::open(file_path).expect("failed to open file");
        let reader = BufReader::new(f);
        let lines = reader
            .lines()
            .collect::<std::io::Result<Vec<String>>>()
            .expect("");
        LocalFileSource {
            lines,
            _marker: PhantomData,
        }
    }
}

impl<A> Source for LocalFileSource<A>
where
    A: ArconType + FromStr,
{
    type Data = A;

    fn process_batch(&mut self, mut ctx: SourceContext<Self, impl ComponentDefinition>) {
        let drain_to = RESCHEDULE_EVERY.min(self.lines.len());
        for line in self.lines.drain(..drain_to) {
            match line.parse::<A>() {
                Ok(data) => ctx.output(data),
                Err(_) => (), // TODO: log parsing error
            }
        }

        if self.lines.is_empty() {
            ctx.signal_end();
        }
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::{ArconF64, ArconType},
        pipeline::Pipeline,
        prelude::{Channel, ChannelStrategy, DebugNode, Forward, Map, NodeID},
    };
    use std::{io::prelude::*, sync::Arc, thread, time};
    use tempfile::NamedTempFile;

    // Shared methods for test cases
    fn wait(time: u64) {
        thread::sleep(time::Duration::from_secs(time));
    }

    fn test_setup<A: ArconType>() -> (Pipeline, Arc<Component<DebugNode<A>>>) {
        let mut pipeline = Pipeline::default();
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

        // Set up SourceContext
        let watermark_interval = 25;
        let backend = std::sync::Arc::new(crate::util::temp_backend());

        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Map::new(Box::new(|x: ArconF64| x)),
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
*/
