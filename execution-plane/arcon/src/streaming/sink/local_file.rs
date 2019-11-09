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
    in_channels: Vec<NodeID>,
}

impl<A> LocalFileSink<A>
where
    A: ArconType + 'static,
{
    pub fn new(file_path: &str, in_channels: Vec<NodeID>) -> Self {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .expect("Failed to open file");

        LocalFileSink {
            ctx: ComponentContext::new(),
            file,
            in_channels,
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
    type Message = ArconMessage<A>;

    fn receive_local(&mut self, msg: Self::Message) {
        if self.in_channels.contains(&msg.sender) {
            debug!(self.ctx.log(), "Got event {:?}", msg.event);
            self.handle_event(&msg.event);
        }
    }
    fn receive_network(&mut self, _msg: NetMessage) {
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
        let system = KompactConfig::default().build().expect("KompactSystem");

        let file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        let node_id = NodeID::new(1);
        let sink_comp = system.create_and_start(move || {
            let sink: LocalFileSink<i32> = LocalFileSink::new(&file_path, vec![node_id]);
            sink
        });
        let input_one = ArconMessage::element(6 as i32, None, node_id);
        let input_two = ArconMessage::element(2 as i32, None, node_id);
        let input_three = ArconMessage::element(15 as i32, None, node_id);
        let input_four = ArconMessage::element(30 as i32, None, node_id);

        let target_ref: ActorRefStrong<ArconMessage<i32>> =
            sink_comp.actor_ref().hold().expect("Failed to fetch");
        target_ref.tell(input_one);
        target_ref.tell(input_two);
        target_ref.tell(input_three);
        target_ref.tell(input_four);

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
