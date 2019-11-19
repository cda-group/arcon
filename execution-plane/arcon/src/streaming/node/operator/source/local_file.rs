use crate::data::NodeID;
use crate::data::{ArconMessage, ArconType};
use crate::streaming::channel::strategy::ChannelStrategy;
use kompact::prelude::*;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;
/*
    LocalFileSource:
    Allows generation of events from a file.
    Takes file path and parameters for how to parse it and target for where to send events.

    Each line of the file should contain just one value of type A which supports FromStr
*/
#[derive(ComponentDefinition)]
pub struct LocalFileSource<A: 'static + ArconType + FromStr> {
    ctx: ComponentContext<LocalFileSource<A>>,
    channel_strategy: Box<dyn ChannelStrategy<A>>,
    file_path: String,
    watermark_interval: u64, // If 0: no watermarks/timestamps generated
    id: NodeID,
}

impl<A: ArconType + FromStr> LocalFileSource<A> {
    pub fn new(
        file_path: String,
        strategy: Box<dyn ChannelStrategy<A>>,
        watermark_interval: u64,
        id: NodeID,
    ) -> LocalFileSource<A> {
        LocalFileSource {
            ctx: ComponentContext::new(),
            channel_strategy: strategy,
            file_path,
            watermark_interval,
            id,
        }
    }
    pub fn process_file(&mut self) {
        if let Ok(f) = File::open(&self.file_path) {
            let reader = BufReader::new(f);
            let mut counter: u64 = 0;

            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if let Ok(v) = l.parse::<A>() {
                            match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                                Ok(ts) => {
                                    if let Err(err) = self.channel_strategy.output(
                                        ArconMessage::element(v, Some(ts.as_secs()), self.id),
                                        &self.ctx.system(),
                                    ) {
                                        error!(
                                            self.ctx.log(),
                                            "Unable to output event, error {}", err
                                        );
                                    } else {
                                        counter += 1;
                                        if counter == self.watermark_interval {
                                            let _ = self.channel_strategy.output(
                                                ArconMessage::watermark(ts.as_secs(), self.id),
                                                &self.ctx.system(),
                                            );
                                            counter = 0;
                                        }
                                    }
                                }
                                _ => {
                                    error!(self.ctx.log(), "Failed to read SystemTime");
                                }
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

            // We finished processing the file
            // Just generate watermarks in a periodic fashion..
            self.schedule_periodic(
                Duration::from_secs(0),
                Duration::from_secs(3),
                move |self_c, _| {
                    self_c.output_watermark();
                },
            );
        } else {
            error!(self.ctx.log(), "Unable to open file {}", self.file_path);
        }
    }

    pub fn output_watermark(&mut self) {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(ts) => {
                if let Err(err) = self.channel_strategy.output(
                    ArconMessage::watermark(ts.as_secs(), self.id),
                    &self.ctx.system(),
                ) {
                    error!(self.ctx.log(), "Unable to output watermark, error {}", err);
                }
            }
            _ => {
                error!(self.ctx.log(), "Failed to read SystemTime");
            }
        }
    }
}

impl<A: ArconType + FromStr> Provide<ControlPort> for LocalFileSource<A> {
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            self.process_file();
        }
    }
}

impl<A: ArconType + FromStr> Actor for LocalFileSource<A> {
    type Message = Box<dyn Any + Send>;
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{Channel, DebugNode, Forward};
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
    fn local_file_u64_no_decimal() {
        let (system, sink) = test_setup::<u64>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        file.write_all(b"123").unwrap();

        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy = Box::new(Forward::new(channel));
        let file_source: LocalFileSource<u64> =
            LocalFileSource::new(String::from(&file_path), channel_strategy, 5, 1.into());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        let r0 = &sink_inspect.data[0];
        assert_eq!(r0.data, 123 as u64);
    }

    #[test]
    fn local_file_u64_decimal() {
        // Should not work, Asserts that nothing is received by sink
        let (system, sink) = test_setup::<u64>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        file.write_all(b"123.5").unwrap();

        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy = Box::new(Forward::new(channel));

        let file_source: LocalFileSource<u64> =
            LocalFileSource::new(String::from(&file_path), channel_strategy, 5, 1.into());
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

        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy = Box::new(Forward::new(channel));

        let file_source: LocalFileSource<f32> =
            LocalFileSource::new(String::from(&file_path), channel_strategy, 5, 1.into());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        let r0 = &sink_inspect.data[0];
        assert_eq!(r0.data, 123 as f32);
    }

    #[test]
    fn local_file_f32_decimal() {
        let (system, sink) = test_setup::<f32>();
        let mut file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        file.write_all(b"123.5").unwrap();

        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy = Box::new(Forward::new(channel));

        let file_source: LocalFileSource<f32> =
            LocalFileSource::new(String::from(&file_path), channel_strategy, 5, 1.into());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        let r0 = &sink_inspect.data[0];
        assert_eq!(r0.data, 123.5 as f32);
    }
}
