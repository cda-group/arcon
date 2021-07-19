// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod filter;
pub mod flatmap;
pub mod map;
pub mod map_in_place;

pub use filter::Filter;
pub use flatmap::FlatMap;
pub use map::Map;
pub use map_in_place::MapInPlace;

#[cfg(test)]
mod tests {
    use crate::prelude::*;

    fn wait(millis: u64) {
        std::thread::sleep(std::time::Duration::from_millis(millis));
    }

    #[test]
    fn map_test() {
        let pipeline = Pipeline::default()
            .with_debug_node()
            .collection((0..10).collect::<Vec<u64>>(), |conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .map(|x| x + 10)
            .build();
        check_map_result(pipeline);
    }

    #[test]
    fn map_in_place_test() {
        let pipeline = Pipeline::default()
            .with_debug_node()
            .collection((0..10).collect::<Vec<u64>>(), |conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .map_in_place(|x| *x += 10)
            .build();

        check_map_result(pipeline);
    }

    // helper to check common result between Map/MapInPlace
    fn check_map_result(mut pipeline: AssembledPipeline) {
        pipeline.start();
        wait(250);

        let debug_node = pipeline.get_debug_node::<u64>().unwrap();

        debug_node.on_definition(|cd| {
            let sum: u64 = cd.data.iter().map(|elem| elem.data).sum();
            assert_eq!(sum, 145);
        });
    }

    #[test]
    fn filter_test() {
        let mut pipeline = Pipeline::default()
            .with_debug_node()
            .collection((0..10).collect::<Vec<u64>>(), |conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .filter(|x| *x < 5)
            .build();

        pipeline.start();

        wait(250);

        let debug_node = pipeline.get_debug_node::<u64>().unwrap();

        debug_node.on_definition(|cd| {
            assert_eq!(cd.data.len(), 5);
        });
    }

    #[test]
    fn flatmap_test() {
        let mut pipeline = Pipeline::default()
            .with_debug_node()
            .collection((0..5).collect::<Vec<u64>>(), |conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .flatmap(|x| (0..x))
            .build();

        pipeline.start();

        wait(250);

        let debug_node = pipeline.get_debug_node::<u64>().unwrap();

        debug_node.on_definition(|cd| {
            assert_eq!(cd.data.len(), 10);
            let sum: u64 = cd.data.iter().map(|elem| elem.data).sum();
            assert_eq!(sum, 10);
        });
    }
}
