use crate::prelude::*;
use crate::control_plane::distributed::*;
use crate::control_plane::distributed::application_controller::*;
use crate::control_plane::distributed::process_controller::*;
use std::time::Duration;

const DEFAULT_WAIT_TIME: u64 = 2000;
const REGISTRATION_TIMEOUT: Duration = Duration::from_millis(2000);

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

fn application() -> Application {
    let mut app_conf = ApplicationConf::default();
    app_conf.kompact_network_host = Some("127.0.0.1:0".to_string());

    Application::with_conf(app_conf)
}

/// Builds a simple DistributedApplication with a defined pipeline.
/// Starts a KompactSystem but does not start any operators or controllers.
fn simple_pipeline(application: Application) -> AssembledApplication {
    let input_range = 0u64..=1010;
    let expected_sum = (0u64..=1000).sum::<u64>();

    application
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
        .build()
}


#[test]
fn adam() -> ArconResult<()> {
    let mut controlling_app = application();
    let mut worker_app = application();

    let layout = Layout::new();

    controlling_app.set_layout(layout.clone());
    worker_app.set_layout(layout);
    
    let assembled_controlling_app = simple_pipeline(controlling_app);
    wait(DEFAULT_WAIT_TIME);
    
    if let Some(application_controller_path) = assembled_controlling_app.get_application_controller() {
        assert!(true);
        worker_app.set_application_controller(application_controller_path);
        let assembled_worker_app = simple_pipeline(worker_app);
    }
    wait(DEFAULT_WAIT_TIME);
    
    /*
    app.data_system().start(&app_controller);
    
    let app_controller_path = app.data_system().actor_path_for(&app_controller);
    
    wait(DEFAULT_WAIT_TIME);
    
    let (process_controller, rf) = app_clone.data_system().create_and_register(|| {
        ProcessController::new(0, app_controller_path, "app_name".to_string())
    });
    let _ = rf.wait_timeout(REGISTRATION_TIMEOUT).expect("registration failed");

    app_clone.data_system().start(&process_controller);

    wait(DEFAULT_WAIT_TIME*5);
    app.start();

    let debug_node = app.get_debug_node::<u64>().unwrap();

    debug_node.on_definition(|cd| {
        let sum: u64 = cd.data.iter().map(|elem| elem.data).sum();
        assert_eq!(sum, expected_sum);
    });
    */

    Ok(())
}
