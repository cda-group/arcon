#![allow(warnings)]
/// Early PoC
extern crate runtime;
extern crate rand;

use runtime::streaming::task::stateless::StreamTask;
use runtime::streaming::task::Destination;
use runtime::prelude::*;
use runtime::util::*;
use runtime::weld::module::*;
use runtime::weld::util::*;
use runtime::weld::*;
use rand::Rng;
use std::sync::Arc;

/// Filter -> Map -> Source
/// Filter(a > 5) -> Map(a + 5) -> Sink(len(vec[i32])
fn main() {
    let (system, remote) = {
        let system = || {
            let mut cfg = KompactConfig::new();
            cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
            KompactSystem::new(cfg).expect("KompactSystem")
        };
        (system(), system())
    };

    let map_task_id = String::from("map_add_5");
    let filter_task_id = String::from("filter_over_5");

    // Define filter task
    let code = String::from("|v: vec[i32]| filter(v, |a:i32| a > 5)");
    let filter_task = filter_task(code, filter_task_id, map_task_id.clone(), &system, &remote);

    // Define map task
    let code = String::from("|v: vec[i32]| map(v, |a:i32| a + i32(5))");
    let map_task = map_task(code, map_task_id, &system, &remote);

    // Define Sink
    let code = String::from("|v: vec[i32]| len(v)");
    let raw_code = generate_raw_module(code.to_string(), false).unwrap();
    let prio = 0;
    let sink_module = Module::new("sink".to_string(), raw_code, prio, None).unwrap();
    let (sink_task, _) = system.create_and_register(move || Sink::new(sink_module));
    let _ = remote.register_by_alias(&sink_task, "sink");

    system.start(&filter_task);
    remote.start(&map_task);
    remote.start(&sink_task);
    std::thread::sleep(std::time::Duration::from_millis(300));

    let filter = ActorPath::Named(NamedPath::with_system(
        system.system_path(),
        vec!["filter_over_5".clone().into()],
    ));

    // The in-house Source
    // Generates random i32 values between 1-15 into a Vec
    //
    // First we need a module "at the source" that serailizes the
    // data into the expected format at the filter task.
    let filter_code = String::from("|v: vec[i32]| filter(v, |a:i32| a > 5)");
    let mut filter_module = Module::new("input".to_string(), filter_code.clone(), 0, None).unwrap();
    filter_module.add_serializer(filter_code).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    for _i in 0..3 {
        let msg = generate_element(&mut filter_module);
        filter.tell(msg, &system);
    }

    system.await_termination();
}

fn filter_task(
    code: String,
    filter_task_id: String,
    map_task_id: String,
    system: &KompactSystem,
    remote: &KompactSystem,
) -> Arc<runtime::prelude::Component<StreamTask>> {
    let raw_code = generate_raw_module(code.to_string(), true).unwrap();
    let prio = 0;
    let filter_module = Module::new(filter_task_id.clone(), raw_code, prio, None).unwrap();

    let map_path = ActorPath::Named(NamedPath::with_system(
        remote.system_path(),
        vec![map_task_id.clone().into()],
    ));

    let map_destination = Destination::new(map_path, map_task_id.clone());

    let (filter_task, _) = system.create_and_register(move || {
        StreamTask::new(
            filter_task_id,
            filter_module,
            Some(map_destination),
        )
    });

    let _ = system.register_by_alias(&filter_task, "filter_over_5");

    filter_task
}

fn map_task(
    code: String,
    map_task_id: String,
    system: &KompactSystem,
    remote: &KompactSystem,
) -> Arc<runtime::prelude::Component<StreamTask>> {
    let raw_code = generate_raw_module(code.to_string(), true).unwrap();
    let prio = 0;
    let map_module = Module::new(map_task_id.clone(), raw_code, prio, None).unwrap();

    let sink_path = ActorPath::Named(NamedPath::with_system(
        remote.system_path(),
        vec!["sink".into()],
    ));

    let sink_destination = Destination::new(sink_path, "sink".to_string());

    let (map_task, _) = system.create_and_register(move || {
        StreamTask::new(
            map_task_id.clone(),
            map_module,
            Some(sink_destination),
        )
    });

    let _ = remote.register_by_alias(&map_task, "map_add_5");
    map_task
}

fn generate_element(filter_module: &mut Module) -> StreamTaskMessage {
    let mut msg = StreamTaskMessage::new();
    let mut element = Element::new();
    element.set_timestamp(get_system_time());
    element.set_task_id("filter_over_5".to_string());

    let mut rng = rand::thread_rng();
    let numbers: Vec<i32> = (0..10).map(|_| rng.gen_range(1, 15)).collect();
    println!("Generated numbers {:?}", numbers);
    let input = WeldVec::from(&numbers);

    let ref mut ctx = WeldContext::new(&filter_module.conf()).unwrap();
    let raw: Vec<u8> = filter_module.serialize_input(&input, ctx).unwrap();
    element.set_data(raw);
    msg.set_element(element);

    msg
}

#[derive(ComponentDefinition)]
pub struct Sink {
    ctx: ComponentContext<Sink>,
    udf: Module,
}

impl Sink {
    pub fn new(module: Module) -> Sink {
        Sink {
            ctx: ComponentContext::new(),
            udf: module,
        }
    }
}

impl Provide<ControlPort> for Sink {
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl Actor for Sink {
    fn receive_local(&mut self, _sender: ActorRef, _msg: &Any) {}
    fn receive_message(&mut self, _sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let msg: StreamTaskMessage = ProtoSer::deserialise(buf).unwrap();
            match msg.payload.unwrap() {
                StreamTaskMessage_oneof_payload::watermark(_) => {}
                StreamTaskMessage_oneof_payload::element(e) => {
                    let raw = e.get_data();
                    let input: WeldVec<u8> =
                        WeldVec::new(raw.as_ref().as_ptr(), raw.as_ref().len() as i64);

                    let ref mut ctx = WeldContext::new(&self.udf.conf()).unwrap();
                    let run: ModuleRun<i64> = self.udf.run(&input, ctx).unwrap();
                    info!(self.ctx.log(), "SINK result with len {}", run.0);
                }
                StreamTaskMessage_oneof_payload::keyed_element(_) => {}
                StreamTaskMessage_oneof_payload::checkpoint(_) => {}
            }
        }
    }
}
