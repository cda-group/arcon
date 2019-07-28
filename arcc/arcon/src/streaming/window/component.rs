use crate::data::{ArconElement, ArconType};
use crate::error::*;
use crate::messages::protobuf::WindowMessage_oneof_payload::*;
use crate::messages::protobuf::*;
use crate::streaming::channel::strategy::ChannelStrategy;
use crate::streaming::window::builder::*;
use kompact::*;
use std::sync::{Arc, Mutex};

/// For each Streaming window, a WindowComponent is spawned
///
/// A: Input event
/// B: ´WindowBuilder`s internal builder type
/// C: Output of Window
/// D: Port type for the `ChannelStrategy`
pub struct WindowComponent<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    ctx: ComponentContext<Self>,
    builder: WindowBuilder<A, B, C>,
    channel_strategy: Arc<Mutex<ChannelStrategy<C, D, Self>>>,
    complete: bool,
    timestamp: u64,
}

impl<A, B, C, D> ComponentDefinition for WindowComponent<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    fn setup(&mut self, self_component: Arc<Component<Self>>) -> () {
        self.ctx_mut().initialise(self_component);
    }
    fn execute(&mut self, _max_events: usize, skip: usize) -> ExecuteResult {
        ExecuteResult::new(skip, skip)
    }
    fn ctx(&self) -> &ComponentContext<Self> {
        &self.ctx
    }
    fn ctx_mut(&mut self) -> &mut ComponentContext<Self> {
        &mut self.ctx
    }
    fn type_name() -> &'static str {
        "WindowComponent"
    }
}

impl<A, B, C, D> WindowComponent<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    pub fn new(
        channel_strategy: Arc<Mutex<ChannelStrategy<C, D, Self>>>,
        window_modules: WindowModules,
        ts: u64,
    ) -> Self {
        let window_builder = WindowBuilder::new(window_modules).unwrap();

        WindowComponent {
            ctx: ComponentContext::new(),
            builder: window_builder,
            channel_strategy: channel_strategy.clone(),
            complete: false,
            timestamp: ts,
        }
    }
    fn add_value(&mut self, v: A) {
        if let Err(_err) = self.builder.on_element(v) {
            error!(self.ctx.log(), "Failed to process element",);
        }

        if self.complete {
            debug!(self.ctx.log(), "Late-value, sending new result",);
            self.trigger();
        }
    }

    fn handle_window_message(&mut self, msg: &WindowMessage) -> () {
        if let Some(payload) = msg.payload.as_ref() {
            match payload {
                element(_) => {
                    // TODO: Handle elements from remote source
                    //self.add_value(e.data); <- Doesn't work because element isn't generic
                }
                keyed_element(_) => {}
                trigger(_) => {
                    self.complete = true;
                    self.trigger();
                }
                complete(_) => {
                    // Unused for now
                }
            }
        } else {
            debug!(self.ctx.log(), "Window received empty WindowMessage")
        }
    }

    fn trigger(&mut self) {
        if let Err(err) = self.output_window() {
            error!(self.ctx.log(), "Failed to complete window with err {}", err);
        }
    }

    fn output_window(&mut self) -> ArconResult<()> {
        let result = self.builder.result()?;
        let self_ptr = self as *const Self;
        let mut p = self.channel_strategy.lock().unwrap();
        let _ = p.output(ArconElement::new(result), self_ptr, None);
        Ok(())
    }
}

impl<A, B, C, D> Provide<ControlPort> for WindowComponent<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                debug!(self.ctx.log(), "Window {} started", self.timestamp);
            }
            ControlEvent::Kill => {
                debug!(self.ctx.log(), "Window {} being killed", self.timestamp);
                if !self.complete {
                    // Trigger result if window wasn't complete when killed.
                    self.trigger();
                }
            }
            ControlEvent::Stop => {}
        }
    }
}

impl<A, B, C, D> Actor for WindowComponent<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(payload) = msg.downcast_ref::<ArconElement<A>>() {
            // "Normal message"
            self.add_value(payload.data.clone());
        } else if let Some(wm) = msg.downcast_ref::<WindowMessage>() {
            self.handle_window_message(wm);
        } else {
            error!(
                self.ctx.log(),
                "Window {} bad local message {:?} from {}", self.timestamp, msg, _sender
            );
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r: Result<WindowMessage, SerError> = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                let _ = self.handle_window_message(&msg);
            } else {
                error!(self.ctx.log(), "Failed to deserialise WindowMessage",);
            }
        } else {
            error!(
                self.ctx.log(),
                "Window {} bad remote message from {}", self.timestamp, sender
            );
        }
    }
}

impl<A, B, C, D> Require<D> for WindowComponent<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    fn handle(&mut self, _event: D::Indication) -> () {
        // ignore
    }
}
