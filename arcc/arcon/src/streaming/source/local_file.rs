use crate::data::ArconType;
use kompact::*;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::str::FromStr;
use std::sync::Arc;
/*
    LocalFileSource:
    Allows generation of events from a file.
    Takes file path and parameters for how to parse it and target for where to send events.

    Each line of the file should contain just one value of type A which supports FromStr
*/
#[derive(ComponentDefinition)]
pub struct LocalFileSource<A: 'static + ArconType + FromStr> {
    ctx: ComponentContext<LocalFileSource<A>>,
    subscriber: Arc<ActorRef>,
    file_path: String,
}

impl<A: ArconType + FromStr> LocalFileSource<A> {
    pub fn new(file_path: String, subscriber: ActorRef) -> LocalFileSource<A> {
        LocalFileSource {
            ctx: ComponentContext::new(),
            subscriber: Arc::new(subscriber),
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
                            self.subscriber.tell(Box::new(v), &self.actor_ref());
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
            _ => {
                error!(self.ctx.log(), "bad ControlEvent");
            }
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
    use kompact::default_components::DeadletterBox;
    use std::io::prelude::*;
    use std::{fs, thread, time};

    // Stub for window-results
    mod sink {
        use super::*;

        pub struct Sink<A: 'static + ArconType> {
            ctx: ComponentContext<Sink<A>>,
            pub result: Vec<A>,
        }
        impl<A: ArconType> Sink<A> {
            pub fn new(_t: A) -> Sink<A> {
                Sink {
                    ctx: ComponentContext::new(),
                    result: Vec::new(),
                }
            }
        }
        impl<A: ArconType> Provide<ControlPort> for Sink<A> {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl<A: ArconType> Actor for Sink<A> {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                println!("sink received message");
                if let Some(m) = msg.downcast_ref::<A>() {
                    println!("trying to push");
                    self.result.push((*m).clone());
                } else {
                    println!("unrecognized message");
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
        impl<A: ArconType> ComponentDefinition for Sink<A> {
            fn setup(&mut self, self_component: Arc<Component<Self>>) -> () {
                self.ctx_mut().initialise(self_component);
            }
            fn execute(&mut self, _max_events: usize, skip: usize) -> ExecuteResult {
                ExecuteResult::new(skip, skip)
            }
            fn ctx(&self) -> &ComponentContext<Self> {
                &self.ctx
            }
            fn ctx_mut(&mut self) -> &mut ComponentContext<Self> {
                &mut self.ctx
            }
            fn type_name() -> &'static str {
                "EventTimeWindowAssigner"
            }
        }
    }
    // Shared methods for test cases
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }
    fn test_setup<A: ArconType>(
        a: A,
    ) -> (
        kompact::KompactSystem,
        Arc<kompact::Component<sink::Sink<A>>>,
    ) {
        // Kompact set-up
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::new(a));

        system.start(&sink);

        return (system, sink);
    }
    // Test cases
    #[test]
    fn local_file_u64_no_decimal() -> std::result::Result<(), std::io::Error> {
        let (system, sink) = test_setup(1 as u64);
        if let Ok(mut file) = File::create("local_file_u64_no_decimal.txt") {
            if let Ok(_) = file.write_all(b"123") {
            } else {
                println!("Unable to write file in test case");
            }
        } else {
            println!("Unable to create file in test case");
        }

        let file_source: LocalFileSource<u64> = LocalFileSource::new(
            String::from("local_file_u64_no_decimal.txt"),
            sink.actor_ref(),
        );
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.result.len(), &(1 as usize));
        let r0 = &sink_inspect.result[0];
        assert_eq!(*r0, 123 as u64);
        fs::remove_file("local_file_u64_no_decimal.txt")?;
        Ok(())
    }
    #[test]
    fn local_file_u64_decimal() -> std::result::Result<(), std::io::Error> {
        // Should not work, Asserts that nothing is received by sink
        let (system, sink) = test_setup(1 as u64);
        if let Ok(mut file) = File::create("local_file_u64_decimal.txt") {
            if let Ok(_) = file.write_all(b"123.5") {
            } else {
                println!("Unable to write file in test case");
            }
        } else {
            println!("Unable to create file in test case");
        }

        let file_source: LocalFileSource<u64> =
            LocalFileSource::new(String::from("local_file_u64_decimal.txt"), sink.actor_ref());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.result.len(), &(0 as usize));
        fs::remove_file("local_file_u64_decimal.txt")?;
        Ok(())
    }
    #[test]
    fn local_file_f32_no_decimal() -> std::result::Result<(), std::io::Error> {
        let (system, sink) = test_setup(1 as f32);
        if let Ok(mut file) = File::create("local_file_f32_no_decimal.txt") {
            if let Ok(_) = file.write_all(b"123") {
            } else {
                println!("Unable to write file in test case");
            }
        } else {
            println!("Unable to create file in test case");
        }

        let file_source: LocalFileSource<f32> = LocalFileSource::new(
            String::from("local_file_f32_no_decimal.txt"),
            sink.actor_ref(),
        );
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.result.len(), &(1 as usize));
        let r0 = &sink_inspect.result[0];
        assert_eq!(*r0, 123 as f32);
        fs::remove_file("local_file_f32_no_decimal.txt")?;
        Ok(())
    }
    #[test]
    fn local_file_f32_decimal() -> std::result::Result<(), std::io::Error> {
        let (system, sink) = test_setup(1 as f32);
        if let Ok(mut file) = File::create("local_file_f32_decimal.txt") {
            if let Ok(_) = file.write_all(b"123.5") {
            } else {
                println!("Unable to write file in test case");
            }
        } else {
            println!("Unable to create file in test case");
        }

        let file_source: LocalFileSource<f32> =
            LocalFileSource::new(String::from("local_file_f32_decimal.txt"), sink.actor_ref());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.result.len(), &(1 as usize));
        let r0 = &sink_inspect.result[0];
        assert_eq!(*r0, 123.5 as f32);
        fs::remove_file("local_file_f32_decimal.txt")?;
        Ok(())
    }
}
