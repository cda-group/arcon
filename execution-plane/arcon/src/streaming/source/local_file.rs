use crate::data::{ArconElement, ArconEvent, ArconType};
use crate::streaming::channel::strategy::ChannelStrategy;
use kompact::*;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::str::FromStr;
/*
    LocalFileSource:
    Allows generation of events from a file.
    Takes file path and parameters for how to parse it and target for where to send events.

    Each line of the file should contain just one value of type A which supports FromStr
*/
#[derive(ComponentDefinition)]
pub struct LocalFileSource<A: 'static + ArconType + FromStr> {
    ctx: ComponentContext<LocalFileSource<A>>,
    channel_strategy: Box<ChannelStrategy<A>>,
    file_path: String,
}

impl<A: ArconType + FromStr> LocalFileSource<A> {
    pub fn new(file_path: String, strategy: Box<ChannelStrategy<A>>) -> LocalFileSource<A> {
        LocalFileSource {
            ctx: ComponentContext::new(),
            channel_strategy: strategy,
            file_path: file_path,
        }
    }
    pub fn process_file(&mut self) {
        if let Ok(f) = File::open(&self.file_path) {
            let reader = BufReader::new(f);

            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if let Ok(v) = l.parse::<A>() {
                            let event = ArconEvent::Element(ArconElement::new(v));
                            if let Err(err) =
                                self.channel_strategy.output(event, &self.ctx.system())
                            {
                                error!(self.ctx.log(), "Unable to output event, error {}", err);
                            }
                        } else {
                            error!(self.ctx.log(), "Unable to parse line {}", self.file_path);
                        }
                    }
                    _ => {
                        error!(self.ctx.log(), "Unable to read line {}", self.file_path);
                    }
                }
            }
        } else {
            error!(self.ctx.log(), "Unable to open file {}", self.file_path);
        }
    }
}

impl<A: ArconType + FromStr> Provide<ControlPort> for LocalFileSource<A> {
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                self.process_file();
            }
            _ => (),
        }
    }
}

impl<A: ArconType + FromStr> Actor for LocalFileSource<A> {
    fn receive_local(&mut self, _sender: ActorRef, _msg: &Any) {}

    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::DebugSink;
    use crate::prelude::{Channel, Forward};
    use kompact::default_components::DeadletterBox;
    use std::io::prelude::*;
    use std::sync::Arc;
    use std::{thread, time};
    use tempfile::NamedTempFile;

    // Shared methods for test cases
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }

    fn test_setup<A: ArconType>() -> (
        kompact::KompactSystem,
        Arc<kompact::Component<DebugSink<A>>>,
    ) {
        // Kompact set-up
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || {
            let s: DebugSink<A> = DebugSink::new();
            s
        });

        system.start(&sink);

        return (system, sink);
    }
    // Test cases
    #[test]
    fn local_file_u64_no_decimal() {
        let (system, sink) = test_setup::<u64>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        file.write_all(b"123").unwrap();

        let channel = Channel::Local(sink.actor_ref());
        let channel_strategy = Box::new(Forward::new(channel));

        let file_source: LocalFileSource<u64> =
            LocalFileSource::new(String::from(&file_path), channel_strategy);
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        let r0 = &sink_inspect.data[0];
        assert_eq!(*r0, 123 as u64);
    }

    #[test]
    fn local_file_u64_decimal() {
        // Should not work, Asserts that nothing is received by sink
        let (system, sink) = test_setup::<u64>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        file.write_all(b"123.5").unwrap();

        let channel = Channel::Local(sink.actor_ref());
        let channel_strategy = Box::new(Forward::new(channel));

        let file_source: LocalFileSource<u64> =
            LocalFileSource::new(String::from(&file_path), channel_strategy);
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(0 as usize));
    }

    #[test]
    fn local_file_f32_no_decimal() {
        let (system, sink) = test_setup::<f32>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        file.write_all(b"123").unwrap();

        let channel = Channel::Local(sink.actor_ref());
        let channel_strategy = Box::new(Forward::new(channel));

        let file_source: LocalFileSource<f32> =
            LocalFileSource::new(String::from(&file_path), channel_strategy);
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        let r0 = &sink_inspect.data[0];
        assert_eq!(*r0, 123 as f32);
    }

    #[test]
    fn local_file_f32_decimal() {
        let (system, sink) = test_setup::<f32>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        file.write_all(b"123.5").unwrap();

        let channel = Channel::Local(sink.actor_ref());
        let channel_strategy = Box::new(Forward::new(channel));

        let file_source: LocalFileSource<f32> =
            LocalFileSource::new(String::from(&file_path), channel_strategy);
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        let r0 = &sink_inspect.data[0];
        assert_eq!(*r0, 123.5 as f32);
    }
}
