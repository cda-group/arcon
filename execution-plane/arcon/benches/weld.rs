#![feature(test)]

extern crate test;

extern crate arcon;
extern crate weld;

use arcon::prelude::*;
use rand::Rng;
use std::sync::Arc;
use weld::*;

#[derive(Clone)]
pub struct Item {
    pub id: u64,
    pub price: u32,
}

#[derive(Clone)]
pub struct SensorData {
    id: u64,
    vec: ArconVec<i32>,
}

#[derive(Clone)]
pub struct EnrichedSensor {
    id: u64,
    total: i32,
}

static ITER_SIZE: usize = 100000;

pub fn item_gen() -> Item {
    let mut rng = rand::thread_rng();
    let id = rng.gen_range(1, 100);
    let price = rng.gen_range(1, 10);
    Item {
        id: id as u64,
        price: price as u32,
    }
}

fn sensor_data() -> SensorData {
    let mut rng = rand::thread_rng();
    let id = rng.gen_range(1, 100);
    let mut values = || (0..ITER_SIZE).map(|_| rng.gen_range(1, 100)).collect();
    let vec: Vec<i32> = values();
    SensorData {
        id: id as u64,
        vec: ArconVec::new(vec),
    }
}

fn weld_mapper(module: &Module, ctx: &mut WeldContext, items: Vec<Item>) {
    let _ = items.iter().map(|i| {
        let run: ModuleRun<Item> = module.run(&i, ctx).unwrap();
        assert_eq!(i.price, run.0.price + 5);
        run.0
    });
}

fn rust_mapper(items: Vec<Item>) {
    let _ = items.iter().map(|i| {
        let item = Item {
            id: i.id,
            price: i.price + 5,
        };
        assert_eq!(i.price, item.price + 5);
        item
    });
}

fn weld_map_filter_reduce(module: &Module, ctx: &mut WeldContext, sensor: &SensorData) {
    let run: ModuleRun<EnrichedSensor> = module.run(sensor, ctx).unwrap();
    assert_eq!(run.0.id, sensor.id);
    assert!(run.0.total > 0);
}

fn rust_map_filter_reduce(sensor: &SensorData) {
    let run: EnrichedSensor = EnrichedSensor {
        id: sensor.id,
        total: sensor.vec.iter().map(|i| i + 5).filter(|x| x > &50).sum(),
    };
    assert_eq!(run.id, sensor.id);
    // To make the compiler not just skip the calculation...
    assert!(run.total > 0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn weld_map_bench(b: &mut Bencher) {
        let code = "|x: u64, y: u32| {x, y + u32(5)}";
        let module = Module::new(code.to_string()).unwrap();
        let items: Vec<Item> = (0..ITER_SIZE).map(|_| item_gen()).collect();
        let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
        b.iter(|| {
            weld_mapper(&module, ctx, items.clone());
        });
    }

    #[bench]
    fn rust_map_bench(b: &mut Bencher) {
        let items: Vec<Item> = (0..ITER_SIZE).map(|_| item_gen()).collect();
        b.iter(|| {
            rust_mapper(items.clone());
        });
    }

    #[bench]
    fn weld_map_filter_reduce_bench(b: &mut Bencher) {
        let code = "|id: u64, x:vec[i32]|
                let m = merger[i32, +];
                let op = for(x, m, |b: merger[i32, +], i, e|  let mapped = e + 5;
                    if(mapped > 50, merge(b, mapped), b));
                {id, result(op)}";
        let module = Module::new(code.to_string()).unwrap();
        let sensor_data = sensor_data();
        let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
        b.iter(|| weld_map_filter_reduce(&module, ctx, &sensor_data));
    }

    #[bench]
    fn rust_map_filter_reduce_bench(b: &mut Bencher) {
        let sensor_data = sensor_data();
        b.iter(|| {
            rust_map_filter_reduce(&sensor_data);
        });
    }

}
