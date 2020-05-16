// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{prelude::*, stream::operator::OperatorContext, timer::TimerBackend};
use arcon_state::{RegistrationToken, Session};
use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::Write,
    marker::PhantomData,
    path::Path,
};

pub struct LocalFileSink<IN>
where
    IN: ArconType,
{
    file: RefCell<File>,
    _marker: PhantomData<IN>,
}

impl<IN> LocalFileSink<IN>
where
    IN: ArconType,
{
    pub fn new(file_path: impl AsRef<Path>) -> Self {
        let file = RefCell::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(file_path)
                .expect("Failed to open file"),
        );

        LocalFileSink {
            file,
            _marker: PhantomData,
        }
    }
}

impl<IN, B> Operator<B> for LocalFileSink<IN>
where
    IN: ArconType,
    B: state::Backend,
{
    type IN = IN;
    type OUT = ArconNever;
    type TimerState = ArconNever;

    fn register_states(&mut self, _registration_token: &mut RegistrationToken<B>) {}

    fn init(&mut self, _session: &mut Session<B>) {}

    fn handle_element(
        &self,
        element: ArconElement<IN>,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
        if let Err(err) = writeln!(self.file.borrow_mut(), "{:?}", element.data) {
            eprintln!("Error while writing to file sink {}", err.to_string());
        }
    }
    fn handle_watermark(
        &self,
        _w: Watermark,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
    }
    fn handle_epoch(
        &self,
        _epoch: Epoch,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
    }
    fn handle_timeout(
        &self,
        _timeout: Self::TimerState,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{prelude::ChannelStrategy, state::InMemory, timer};
    use std::io::{BufRead, BufReader};
    use tempfile::NamedTempFile;

    #[test]
    fn local_file_sink_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        let node_id = NodeID::new(1);
        let sink_comp = system.create(move || {
            Node::new(
                String::from("sink_comp"),
                0.into(),
                vec![node_id],
                ChannelStrategy::Mute,
                LocalFileSink::new(&file_path),
                InMemory::create("test".as_ref()).unwrap(),
                timer::none(),
            )
        });
        system.start(&sink_comp);
        let input_one = ArconMessage::element(6i32, None, node_id);
        let input_two = ArconMessage::element(2i32, None, node_id);
        let input_three = ArconMessage::element(15i32, None, node_id);
        let input_four = ArconMessage::element(30i32, None, node_id);

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
