// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::ArconType,
    stream::source::{Poll, Source},
};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

pub struct LocalFileSource<A>
where
    A: ArconType + FromStr,
{
    lines: std::io::Lines<BufReader<std::fs::File>>,
    _marker: std::marker::PhantomData<A>,
}

impl<A> LocalFileSource<A>
where
    A: ArconType + FromStr,
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
    A: ArconType + FromStr,
{
    type Item = A;

    fn poll_next(&mut self) -> Poll<Self::Item> {
        match self.lines.next() {
            Some(Ok(line)) => match line.parse::<Self::Item>() {
                Ok(record) => Poll::Ready(record),
                Err(_) => Poll::Error("failed to parse line".to_string()),
            },
            Some(Err(err)) => Poll::Error(err.to_string()),
            None => Poll::Done,
        }
    }
    fn set_offset(&mut self, _: usize) {}
}
