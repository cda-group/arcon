use crate::prelude::*;
//use crate::ArconResult;

const DEFAULT_WAIT_TIME: u64 = 10000;

fn wait(millis: u64) {
    std::thread::sleep(std::time::Duration::from_millis(millis));
}

#[test]
fn simple_distributed_pipeline() -> ArconResult<()> {
    // Declare a Pipeline

    // Start a Source/Sink System

    // Start a Controller System

    // Start another System

    // Assert on the Source/Sink System

    Ok(())
}

fn window_sum(buffer: &[u64]) -> u64 {
    buffer.iter().sum()
}

#[test]
fn adam() -> ArconResult<()> {
    let input_range = 0u64..=1010;
    let expected_sum = (0u64..=1000).sum::<u64>();
    let mut app = Application::default()
        .with_debug_node()
        .iterator(input_range, |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .operator(OperatorBuilder {
            operator: Arc::new(|| {
                let conf = WindowConf {
                    assigner: Assigner::Sliding {
                        length: Time::seconds(10),
                        slide: Time::seconds(10),
                        late_arrival: Time::seconds(0),
                    },
                    kind: StreamKind::Keyed,
                };
                WindowAssigner::new(conf)
            }),
            state: Arc::new(|backend| {
                let index = AppenderWindow::new(backend.clone(), &window_sum);
                WindowState::new(index, backend)
            }),
            conf: OperatorConf::default(),
        })
        .to_console()
        .build();

    app.start();
    wait(DEFAULT_WAIT_TIME);

    let debug_node = app.get_debug_node::<u64>().unwrap();

    debug_node.on_definition(|cd| {
        let sum: u64 = cd.data.iter().map(|elem| elem.data).sum();
        assert_eq!(sum, expected_sum);
    });

    Ok(())
}
