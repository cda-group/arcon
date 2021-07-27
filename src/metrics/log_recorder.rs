use crate::application::conf::logger::ArconLogger;
use arcon::prelude::*;
use metrics::{GaugeValue, Key, Recorder, Unit};

pub struct LogRecorder {
    pub(crate) logger: ArconLogger,
}

impl Recorder for LogRecorder {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        info!(
            self.logger,
            "(counter) registered key {} with unit {:?} and description {:?}",
            key,
            unit,
            description
        );
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        info!(
            self.logger,
            "(gauge) registered key {} with unit {:?} and description {:?}", key, unit, description
        );
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        info!(
            self.logger,
            "(histogram) registered key {} with unit {:?} and description {:?}",
            key,
            unit,
            description
        );
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        info!(self.logger, "(counter) got value {} for key {}", value, key);
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        info!(self.logger, "(gauge) got value {:?} for key {}", value, key);
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        info!(
            self.logger,
            "(histogram) got value {} for key {}", value, key
        );
    }
}
