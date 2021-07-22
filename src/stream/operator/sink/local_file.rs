// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconNever, ArconType},
    error::ArconResult,
    stream::operator::{Operator, OperatorContext},
};
use arcon_state::Backend;
use kompact::prelude::*;
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
    op_state: (),
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
            op_state: (),
            _marker: PhantomData,
        }
    }
}

impl<IN> Operator for LocalFileSink<IN>
where
    IN: ArconType,
{
    type IN = IN;
    type OUT = ArconNever;
    type TimerState = ArconNever;
    type OperatorState = ();

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        _ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> ArconResult<()> {
        if let Err(err) = writeln!(self.file.borrow_mut(), "{:?}", element.data) {
            eprintln!("Error while writing to file sink {}", err.to_string());
        }
        Ok(())
    }
    crate::ignore_timeout!();
    crate::ignore_persist!();

    fn state(&mut self) -> &mut Self::OperatorState {
        &mut self.op_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::io::{BufRead, BufReader};
    use tempfile::NamedTempFile;

    #[test]
    fn local_file_sink_test() {
        let file = NamedTempFile::new().unwrap();
        let file_path = file.path().to_string_lossy().into_owned();

        let mut app = Application::default()
            .collection(vec![6i32, 2i32, 15i32, 30i32], |conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .operator(OperatorBuilder {
                constructor: Arc::new(move |_: Arc<Sled>| LocalFileSink::new(&file_path)),
                conf: OperatorConf {
                    parallelism_strategy: ParallelismStrategy::Static(1),
                    ..Default::default()
                },
            })
            .build();

        app.start();

        std::thread::sleep(std::time::Duration::from_secs(1));

        let file = File::open(file.path()).expect("no such file");
        let buf = BufReader::new(file);
        let result: Vec<i32> = buf
            .lines()
            .map(|l| l.unwrap().parse::<i32>().expect("could not parse line"))
            .collect();

        let expected: Vec<i32> = vec![6, 2, 15, 30];
        assert_eq!(result, expected);
    }
}
