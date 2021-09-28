use crate::data::{ArconElement, ArconNever, ArconType};
use crate::error::*;
use crate::index::EmptyState;
use crate::stream::operator::{Operator, OperatorContext};
use slog::*;
use std::marker::PhantomData;

// Throughput logic taken from: https://github.com/lsds/StreamBench/blob/master/yahoo-streaming-benchmark/src/main/scala/uk/ac/ic/imperial/benchmark/flink/YahooBenchmark.scala
pub struct MeasureSink<IN: ArconType> {
    log_freq: u64,
    last_total_recv: u64,
    last_time: u64,
    total_recv: u64,
    avg_throughput: f32,
    throughput_counter: u64,
    throughput_sum: f32,
    _marker: PhantomData<IN>,
}

impl<IN: ArconType> MeasureSink<IN> {
    /// Creates a MeasureSink that logs throughput
    pub fn new(log_freq: u64) -> Self {
        Self {
            log_freq,
            last_total_recv: 0,
            last_time: 0,
            total_recv: 0,
            avg_throughput: 0.0,
            throughput_counter: 0,
            throughput_sum: 0.0,
            _marker: PhantomData,
        }
    }
}

impl<IN: ArconType> Operator for MeasureSink<IN> {
    type IN = IN;
    type OUT = IN;
    type TimerState = ArconNever;
    type OperatorState = EmptyState;
    type ElementIterator = std::iter::Empty<ArconElement<IN>>;

    fn handle_element(
        &mut self,
        _: ArconElement<Self::IN>,
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Self::ElementIterator> {
        // When a latency marker is introduced for ArconEvent, calculate the end-to-end latency and report.
        // https://github.com/cda-group/arcon/issues/235

        if self.total_recv == 0 {
            info!(
                ctx.log(),
                "ThroughputLogging {}, {}",
                crate::util::get_system_time(),
                self.total_recv
            );
        }

        self.total_recv += 1;

        if self.total_recv % self.log_freq == 0 {
            let current_time = crate::util::get_system_time();

            if current_time > self.last_time {
                let throughput = (self.total_recv - self.last_total_recv) as f32
                    / (current_time - self.last_time) as f32
                    * 1000.0;

                if throughput != 0.0 {
                    self.throughput_counter += 1;
                    self.throughput_sum += throughput;
                    self.avg_throughput = self.throughput_sum / self.throughput_counter as f32;
                }

                info!(
                    ctx.log(),
                    "Throughput {}, Average {}", throughput, self.avg_throughput
                );

                info!(
                    ctx.log(),
                    "ThroughputLogging at time {}, with total recv {}",
                    crate::util::get_system_time(),
                    self.total_recv
                );
                self.last_time = current_time;
                self.last_total_recv = self.total_recv;
            }
        }

        Ok(std::iter::empty::<ArconElement<IN>>())
    }

    crate::ignore_timeout!();
}
