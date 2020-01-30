// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{data::ArconEvent, prelude::*};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    marker::PhantomData,
};

pub struct LocalFileSink<IN>
where
    IN: ArconType,
{
    file: File,
    _marker: PhantomData<IN>,
}

impl<IN> LocalFileSink<IN>
where
    IN: ArconType,
{
    pub fn new(file_path: &str) -> Self {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .expect("Failed to open file");

        LocalFileSink {
            file,
            _marker: PhantomData,
        }
    }
}

impl<IN> Operator<IN, IN> for LocalFileSink<IN>
where
    IN: ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<IN>>> {
        if let Err(err) = writeln!(self.file, "{:?}", element.data) {
            eprintln!("Error while writing to file sink {}", err.to_string());
        }
        Ok(Vec::new())
    }
    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<Vec<ArconEvent<IN>>> {
        Ok(Vec::new())
    }
    fn handle_epoch(&mut self, _epoch: Epoch) -> ArconResult<Vec<u8>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::util::mute_strategy;
    use std::io::{BufRead, BufReader};
    use tempfile::NamedTempFile;

    #[test]
    fn local_file_sink_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        let node_id = NodeID::new(1);
        let sink_comp = system.create_and_start(move || {
            Node::new(
                0.into(),
                vec![node_id],
                mute_strategy::<i32>(),
                Box::new(LocalFileSink::new(&file_path)),
            )
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
