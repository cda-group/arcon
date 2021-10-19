use crate::prelude::*;
//use crate::ArconResult;

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
  let mut app = Application::default()
    .iterator(0u64..10, |conf| {
      conf.set_arcon_time(ArconTime::Event);
      conf.set_timestamp_extractor(|x: &u64| *x);
    })
    .operator(OperatorBuilder {
      operator: Arc::new(|| {
        let conf = WindowConf {
          assigner: Assigner::Sliding {
            length: Time::seconds(1000),
            slide: Time::seconds(500),
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
  app.await_termination();
  
  Ok(())
}
