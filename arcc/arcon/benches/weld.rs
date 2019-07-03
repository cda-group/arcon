#![feature(test)]

extern crate test;

extern crate runtime;
extern crate weld;

use rand::Rng;
use runtime::prelude::*;
use runtime::weld::WeldVec;
use std::sync::Arc;
use weld::*;

#[derive(Clone)]
pub struct Item {
    pub id: u64,
    pub price: u32,
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

fn weld_mapper(module: Arc<Module>, ctx: &mut WeldContext, items: Vec<Item>) {
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

fn weld_map_reducer(module: Arc<Module>, ctx: &mut WeldContext, nums: Vec<i32>) {
    let weld_vec = WeldVec::from(&nums);
    let _run: ModuleRun<i32> = module.run(&weld_vec, ctx).unwrap();
}

fn rust_map_reducer(nums: Vec<i32>) {
    let _run: i32 = nums.iter().map(|i| i * 4).sum();
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn weld_map_bench(b: &mut Bencher) {
        let code = "|x: u64, y: u32| {x, y + u32(5)}";
        let module =
            Arc::new(Module::new("mapper".to_string(), code.to_string(), 0, None).unwrap());
        let items: Vec<Item> = (0..ITER_SIZE).map(|_| item_gen()).collect();
        let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
        b.iter(|| {
            weld_mapper(module.clone(), ctx, items.clone());
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
    fn weld_map_reducer_bench(b: &mut Bencher) {
        let code = "|x:vec[i32]|
            result(
                for(
                    map(x, |e| e * 4),
                    merger[i32,+],
                    |b,i,e| merge(b, e)
                    )
                )";
        let module =
            Arc::new(Module::new("map_reduce".to_string(), code.to_string(), 0, None).unwrap());

        let mut rng = rand::thread_rng();
        let values: Vec<i32> = (0..ITER_SIZE).map(|_| rng.gen_range(1, 500)).collect();
        let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
        b.iter(|| weld_map_reducer(module.clone(), ctx, values.clone()));
    }

    #[bench]
    fn rust_map_reducer_bench(b: &mut Bencher) {
        let mut rng = rand::thread_rng();
        let values: Vec<i32> = (0..ITER_SIZE).map(|_| rng.gen_range(1, 500)).collect();
        b.iter(|| {
            rust_map_reducer(values.clone());
        });
    }

}
