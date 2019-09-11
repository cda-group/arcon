pub mod filter;
pub mod flatmap;
pub mod manager;
pub mod map;
pub mod node;

use crate::prelude::*;

pub struct TaskMetric {
    avg: u64,
    executions: u64,
}

impl TaskMetric {
    pub fn new() -> Self {
        TaskMetric {
            avg: 0,
            executions: 0,
        }
    }
    pub fn update_avg(&mut self, ns: u64) {
        if self.executions == 0 {
            self.avg = ns;
        } else {
            let ema: i32 = (ns as i32 - self.avg as i32) * (2 / (self.executions + 1)) as i32
                + self.avg as i32;
            self.avg = ema as u64;
        }
        self.executions += 1;
    }

    pub fn get_avg(&self) -> u64 {
        self.avg
    }
}

pub trait Task<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<OUT>>>;
    fn handle_watermark(&mut self, watermark: Watermark) -> ArconResult<Vec<ArconEvent<OUT>>>;
    fn handle_epoch(&mut self, epoch: Epoch) -> ArconResult<Vec<u8>>;
}
