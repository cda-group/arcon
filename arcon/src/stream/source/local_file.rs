use crate::error::{
    source::{SourceError, SourceResult},
    Error,
};

use crate::{
    data::ArconType,
    stream::source::{Poll, Source},
};
use std::{
    fmt::Display,
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

pub struct LocalFileSource<A>
where
    A: ArconType + FromStr + Display,
    <A as FromStr>::Err: Display,
{
    lines: std::io::Lines<BufReader<std::fs::File>>,
    _marker: std::marker::PhantomData<A>,
}

impl<A> LocalFileSource<A>
where
    A: ArconType + FromStr + Display,
    <A as FromStr>::Err: Display,
{
    pub fn new(file_path: String) -> Self {
        let f = File::open(file_path).expect("failed to open file");
        let reader = BufReader::new(f);
        let lines = reader.lines();
        LocalFileSource {
            lines,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<A> Source for LocalFileSource<A>
where
    A: ArconType + FromStr + Display,
    <A as FromStr>::Err: Display,
{
    type Item = A;

    fn poll_next(&mut self) -> SourceResult<Poll<Self::Item>> {
        match self.lines.next() {
            Some(Ok(line)) => match line.parse::<Self::Item>() {
                Ok(record) => Ok(Ok(Poll::Ready(record))),
                Err(err) => Ok(Err(SourceError::Parse {
                    msg: err.to_string(),
                })),
            },
            Some(Err(err)) => Err(Error::Io { error: err }),
            None => Ok(Ok(Poll::Done)),
        }
    }
    fn set_offset(&mut self, _: usize) {}
}
