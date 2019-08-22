use crate::data::ArconEvent;
use crate::prelude::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;

#[derive(ComponentDefinition)]
pub struct LocalFileSink<A>
where
    A: ArconType + 'static,
{
    ctx: ComponentContext<Self>,
    file: File,
}

impl<A> LocalFileSink<A>
where
    A: ArconType + 'static,
{
    pub fn new(file_path: &str) -> Self {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .expect("Failed to open file");

        LocalFileSink {
            ctx: ComponentContext::new(),
            file,
        }
    }

    fn handle_event(&mut self, event: &ArconEvent<A>) {
        match event {
            ArconEvent::Element(e) => {
                if let Err(err) = writeln!(self.file, "{:?}", e.data) {
                    error!(
                        self.ctx.log(),
                        "Failed to write sink element to file sink with err {}",
                        err.to_string()
                    );
                }
            }
            _ => {}
        }
    }
}

impl<A> Provide<ControlPort> for LocalFileSink<A>
where
    A: ArconType + 'static,
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl<A> Actor for LocalFileSink<A>
where
    A: ArconType + 'static,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(event) = msg.downcast_ref::<ArconEvent<A>>() {
            debug!(self.ctx.log(), "Got event {:?}", event);
            self.handle_event(event);
        }
    }
    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use tempfile::NamedTempFile;

    #[test]
    fn local_file_sink_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        let sink_comp = system.create_and_start(move || {
            let sink: LocalFileSink<i32> = LocalFileSink::new(&file_path);
            sink
        });

        let input_one = ArconElement::new(6 as i32);
        let input_two = ArconElement::new(2 as i32);
        let input_three = ArconElement::new(15 as i32);
        let input_four = ArconElement::new(30 as i32);

        let target_ref = sink_comp.actor_ref();
        target_ref.tell(Box::new(ArconEvent::Element(input_one)), &target_ref);
        target_ref.tell(Box::new(ArconEvent::Element(input_two)), &target_ref);
        target_ref.tell(Box::new(ArconEvent::Element(input_three)), &target_ref);
        target_ref.tell(Box::new(ArconEvent::Element(input_four)), &target_ref);

        std::thread::sleep(std::time::Duration::from_secs(1));

        let file = File::open(file.path()).expect("no such file");
        let buf = BufReader::new(file);
        let result: Vec<i32> = buf
            .lines()
            .map(|l| l.unwrap().parse::<i32>().expect("could not parse line"))
            .collect();

        let expected: Vec<i32> = vec![6, 2, 15, 30];
        assert_eq!(result, expected);
        let _ = system.shutdown();
    }
}
