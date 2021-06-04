// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{ArconResult, Error};
use snafu::Snafu;
use std::fmt::Debug;

/// Nested result type for handling source errors
pub type SourceResult<A> = ArconResult<std::result::Result<A, SourceError>>;

/// Enum containing every type of error that a source may encounter
#[derive(Debug, Snafu)]
pub enum SourceError {
    #[snafu(display("Schema Error Encountered {}", msg))]
    Schema { msg: String },
    #[snafu(display("Failed to parse data {}", msg))]
    Parse { msg: String },
    #[cfg(feature = "kafka")]
    #[snafu(display("Encountered a Kafka error {}", error.to_string()))]
    Kafka { error: rdkafka::error::KafkaError },
}

impl<A> From<Error> for SourceResult<A> {
    fn from(error: Error) -> Self {
        Err(error)
    }
}
