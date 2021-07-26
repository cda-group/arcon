use crate::application::conf::logger::ArconLogger;
use metrics::{GaugeValue, Key, Recorder, Unit};

pub struct LogRecorder {
    pub(crate) logger: ArconLogger,
}

impl Recorder for LogRecorder {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        println!(
            "(counter) registered key {} with unit {:?} and description {:?}",
            key, unit, description
        );
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        println!(
            "(gauge) registered key {} with unit {:?} and description {:?}",
            key, unit, description
        );
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        println!(
            "(histogram) registered key {} with unit {:?} and description {:?}",
            key, unit, description
        );
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        println!("(counter) got value {} for key {}", value, key);
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        println!("(gauge) got value {:?} for key {}", value, key);
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        println!("(histogram) got value {} for key {}", value, key);
    }
}
