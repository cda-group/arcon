use crate::streaming::task::manager::{Metric, MetricPort};
use messages::protobuf::StreamTaskMessage_oneof_payload::*;
use messages::protobuf::*;

use crate::streaming::task::Destination;
use crate::error::Error;
use crate::error::ErrorKind::*;
use crate::weld::*;
use kompact::*;
use std::time::Duration;
use weld::*;

#[derive(ComponentDefinition)]
#[allow(dead_code)]
pub struct StreamTask {
    ctx: ComponentContext<StreamTask>,
    report_timer: Option<ScheduledTimer>,
    pub manager_port: RequiredPort<MetricPort, StreamTask>,
    destination: Option<Destination>,
    udf: Module,
    udf_avg: u64,
    udf_executions: u64,
    id: String,
}

impl StreamTask {
    pub fn new(id: String, udf: Module, destination: Option<Destination>) -> StreamTask {
        StreamTask {
            ctx: ComponentContext::new(),
            report_timer: None,
            manager_port: RequiredPort::new(),
            destination,
            udf,
            udf_avg: 0,
            udf_executions: 0,
            id,
        }
    }

    fn stop_report(&mut self) {
        if let Some(timer) = self.report_timer.clone() {
            self.cancel_timer(timer);
            self.report_timer = None;
        }
    }

    fn handle_msg(&mut self, data: StreamTaskMessage) -> crate::error::Result<()> {
        let payload = data.payload.unwrap();

        match payload {
            element(e) => {
                if let Err(err) = self.run_udf(e) {
                    error!(
                        self.ctx.log(),
                        "Failed to run StreamTask UDF with err: {}",
                        err.to_string()
                    );
                }
            }
            keyed_element(_) => {
                unimplemented!();
            }
            watermark(_) => {
                unimplemented!();
            }
            checkpoint(_) => {
                unimplemented!();
            }
        }

        Ok(())
    }

    fn run_udf(&mut self, e: Element) -> crate::error::Result<()> {
        if e.get_task_id() != self.id {
            let err_fmt = format!(
                "given task id {} does not match {}",
                e.get_task_id(),
                self.id
            );
            Err(Error::new(BadTaskError(err_fmt)))
        } else {
            let raw = e.get_data();
            let input: WeldVec<u8> = WeldVec::new(raw.as_ref().as_ptr(), raw.as_ref().len() as i64);

            let ref mut ctx = WeldContext::new(&self.udf.conf()).map_err(|e| {
                Error::new(ContextError(e.message().to_string_lossy().into_owned()))
            })?;

            let run: ModuleRun<WeldVec<u8>> = self.udf.run(&input, ctx)?;
            let ns = run.1;
            self.update_avg(ns);

            if let Some(dest) = &self.destination {
                let mut msg = StreamTaskMessage::new();
                let mut element_obj = Element::new();
                element_obj.set_timestamp(crate::util::get_system_time());
                element_obj.set_task_id(dest.task_id.clone());
                let to_raw = to_rust_vec(run.0).unwrap();
                element_obj.set_data(to_raw.to_vec());
                msg.set_element(element_obj);

                dest.path.tell(msg, self);
            }

            Ok(())
        }
    }

    fn update_avg(&mut self, ns: u64) {
        if self.udf_executions == 0 {
            self.udf_avg = ns;
        } else {
            let ema: i32 = (ns as i32 - self.udf_avg as i32)
                * (2 / (self.udf_executions + 1)) as i32
                + self.udf_avg as i32;
            self.udf_avg = ema as u64;
        }
        self.udf_executions += 1;
    }
}

impl Provide<ControlPort> for StreamTask {
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                info!(self.ctx.log(), "StreamTask {} Starting up", self.id);
                let timeout = Duration::from_millis(250);
                let timer = self.schedule_periodic(timeout, timeout, |self_c, _| {
                    self_c.manager_port.trigger(Metric {
                        task_id: self_c.id.clone(),
                        task_avg: self_c.udf_avg,
                    });
                });

                self.report_timer = Some(timer);
            }
            ControlEvent::Stop => self.stop_report(),
            ControlEvent::Kill => self.stop_report(),
        }
    }
}

impl Actor for StreamTask {
    fn receive_local(&mut self, _sender: ActorRef, _msg: &Any) {}
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                let _ = self.handle_msg(msg);
            } else {
                error!(self.ctx.log(), "Failed to handle StreamTaskMessage",);
            }
        } else {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }
}

impl Require<MetricPort> for StreamTask {
    fn handle(&mut self, _event: Metric) -> () {
        // ?
    }
}
