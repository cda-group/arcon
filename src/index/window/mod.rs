pub mod appender;
pub mod arrow;
pub mod incremental;

#[cfg(test)]
mod tests {
    use super::appender::*;
    use super::incremental::*;
    use crate::index::WindowIndex;
    use crate::stream::operator::window::WindowContext;
    use crate::test_utils::temp_backend;
    use arcon_state::Sled;
    use std::sync::Arc;

    #[test]
    fn sum_appender_window_test() {
        let backend = Arc::new(temp_backend::<Sled>());

        fn materializer(buffer: &[i32]) -> i32 {
            buffer.iter().sum()
        }

        let mut window = AppenderWindow::new(backend, &materializer);

        for i in 0..10 {
            let _ = window.on_element(i, WindowContext::new(0, 0));
        }

        let sum = window.result(WindowContext::new(0, 0)).unwrap();

        let expected: i32 = 45;
        assert_eq!(sum, expected);
    }

    #[test]
    fn sum_incremental_window_test() {
        let backend = Arc::new(temp_backend::<Sled>());

        fn init(i: i32) -> u64 {
            i as u64
        }
        fn aggregation(i: i32, agg: &u64) -> u64 {
            agg + i as u64
        }

        let mut window = IncrementalWindow::new(backend, &init, &aggregation);

        for i in 0..10 {
            let _ = window.on_element(i, WindowContext::new(0, 0));
        }

        for i in 0..20 {
            let _ = window.on_element(i, WindowContext::new(1, 1));
        }

        let sum_one = window.result(WindowContext::new(0, 0)).unwrap();
        assert_eq!(sum_one, 45);
        let sum_two = window.result(WindowContext::new(1, 1)).unwrap();
        assert_eq!(sum_two, 190);
    }
}
