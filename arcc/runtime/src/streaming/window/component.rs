use crate::streaming::window::assigner::*;
use crate::streaming::window::builder::*;
use kompact::*;
use messages::protobuf::WindowMessage_oneof_payload::*;
use messages::protobuf::*;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use LocalElement;

pub struct WindowComponent<
    A: 'static + Send + Clone + Sync + Debug + Display,
    B: 'static + Clone,
    C: 'static + Send + Clone + Sync + Display,
> {
    ctx: ComponentContext<WindowComponent<A, B, C>>,
    builder: WindowBuilder<A, B, C>,
    target_pointer: ActorRef,
    complete: bool,
    timestamp: u64,
}

// Implement ComponentDefinition
impl<A: Send + Clone + Sync + Debug + Display, B: Clone, C: Send + Clone + Sync + Display>
    ComponentDefinition for WindowComponent<A, B, C>
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

impl<A: Send + Clone + Sync + Debug + Display, B: Clone, C: Send + Clone + Sync + Display>
    WindowComponent<A, B, C>
{
    pub fn new(
        target: ActorRef,
        window_modules: WindowModules,
        ts: u64,
    ) -> WindowComponent<A, B, C> {
        let window_builder: WindowBuilder<A, B, C> = WindowBuilder::new(window_modules).unwrap();

        WindowComponent {
            ctx: ComponentContext::new(),
            builder: window_builder,
            target_pointer: target,
            complete: false,
            timestamp: ts,
        }
    }
    fn add_value(&mut self, v: A) -> () {
        if let Ok(_) = self.builder.on_element(v) {
        } else {
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
    fn trigger(&mut self) -> () {
        // Close the window, only trigger if we weren't already complete
        if let Ok(result) = self.builder.result() {
            debug!(
                self.ctx.log(),
                "materialized result for window {}", self.timestamp
            );
            // Report our result to the target
            self.target_pointer.tell(
                Arc::new(LocalElement {
                    data: result,
                    timestamp: self.timestamp,
                }),
                &self.actor_ref(),
            );
        } else {
            error!(
                self.ctx.log(),
                "Window {} failed to materialize result", self.timestamp
            );
        }
    }
}

impl<A: Send + Clone + Sync + Debug + Display, B: Clone, C: Send + Clone + Sync + Display>
    Provide<ControlPort> for WindowComponent<A, B, C>
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

impl<A: Send + Clone + Sync + Debug + Display, B: Clone, C: Send + Clone + Sync + Display> Actor
    for WindowComponent<A, B, C>
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(payload) = msg.downcast_ref::<LocalElement<A>>() {
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
