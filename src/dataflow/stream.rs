// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{data::ArconType, stream::operator::Operator};
use arcon_error::ArconResult;
use arcon_state::index::ArconState;
use std::marker::PhantomData;

// NOTES.
//
// Configurables:
//
// (1) State Backend; (2) State ID; (3) Parallelism
//
// With Defaults: (1) BackendNever; (3) num_cpus::defaut()?
// Must be chosen?: (2) State ID
//
// (1): stream.map(..).with_backend(RocksDB)
// (2): stream.map(..).state_id("map_state")
// (3): stream.map(..).parallelism(12)

pub struct Stream<IN: ArconType> {
    // TODO: add vec/graph to hold information of current "pipeline"
    _marker: PhantomData<IN>,
}

impl<IN: ArconType> Stream<IN> {
    pub fn map<OUT: ArconType, F: Fn(IN) -> ArconResult<OUT>>(self, f: F) -> Stream<OUT> {
        unimplemented!();
    }

    pub fn map_with_state<OUT, S, F>(self, f: F) -> Stream<OUT>
    where
        OUT: ArconType,
        S: ArconState,
        F: Fn(IN, &mut S) -> ArconResult<OUT>,
    {
        unimplemented!();
    }

    pub fn operator<OP: Operator>(self, operator: OP) -> Stream<OP::OUT> {
        unimplemented!();
    }

    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn just_testing_things() {
        let stream: Stream<u64> = Stream::new();
        let new_stream = stream.map(|x| Ok(x + 1));
    }
}
