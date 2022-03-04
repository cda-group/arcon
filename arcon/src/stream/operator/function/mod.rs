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
        let app = (0..10)
            .to_stream(|conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .map(|x| x + 10)
            .debug()
            .builder()
            .build();

        check_map_result(app);
    }

    #[test]
    fn map_in_place_test() {
        let app = (0..10)
            .to_stream(|conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .map_in_place(|x| *x += 10)
            .debug()
            .builder()
            .build();

        check_map_result(app);
    }

    // helper to check common result between Map/MapInPlace
    fn check_map_result(mut app: Application) {
        app.run();
        wait(1000);

        let debug_node = app.get_debug_node::<i32>().unwrap();

        debug_node.on_definition(|cd| {
            let sum: i32 = cd.data.iter().map(|elem| elem.data).sum();
            assert_eq!(sum, 145);
        });
    }

    #[test]
    fn filter_test() {
        let mut app = (0..10i32)
            .to_stream(|conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .filter(|x| *x < 5)
            .debug()
            .builder()
            .build();

        app.run();
        wait(1000);

        let debug_node = app.get_debug_node::<i32>().unwrap();

        debug_node.on_definition(|cd| {
            assert_eq!(cd.data.len(), 5);
        });
    }

    #[test]
    fn flatmap_test() {
        let mut builder = (0..5i32)
            .to_stream(|conf| {
                conf.set_arcon_time(ArconTime::Process);
            })
            .flat_map(|x| (0..x))
            .debug()
            .builder();

        let mut app = builder.build();

        app.run();
        wait(1000);

        let debug_node = app.get_debug_node::<i32>().unwrap();

        debug_node.on_definition(|cd| {
            assert_eq!(cd.data.len(), 10);
            let sum: i32 = cd.data.iter().map(|elem| elem.data).sum();
            assert_eq!(sum, 10);
        });
    }
}
