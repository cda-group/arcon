use crate::streaming::window::builder::*;
use crate::streaming::window::window_assigner::*;
use crate::weld::module::Module;
use kompact::*;
use messages::protobuf::WindowMessage_oneof_payload::*;
use messages::protobuf::*;
use std::fmt::Debug;
use std::fmt::Display;
use std::string::ToString;
use std::sync::Arc;
use LocalElement;

pub struct WindowComponent<
    A: 'static + Send + Clone + Sync + Debug + Display,
    B: 'static + Clone,
    C: 'static + Send + Clone + Sync + Display,
> {
    ctx: ComponentContext<WindowComponent<A, B, C>>,
    builder: WindowBuilder<A, B, C>,
    targetPointer: ActorRef,
    id: u64,
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
    fn execute(&mut self, max_events: usize, skip: usize) -> ExecuteResult {
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
        init_builder: Arc<Module>,
        code_module: Arc<Module>,
        result_module: Arc<Module>,
        id: u64,
        ts: u64,
    ) -> WindowComponent<A, B, C> {
        let window_modules = WindowModules {
            init_builder,
            udf: code_module,
            materializer: result_module,
        };

        let mut window_builder: WindowBuilder<A, B, C> =
            WindowBuilder::new(window_modules).unwrap();

        WindowComponent {
            ctx: ComponentContext::new(),
            builder: window_builder,
            targetPointer: target,
            id: id,
            complete: false,
            timestamp: ts,
        }
    }
    fn add_value(&mut self, v: A) -> () {
        self.builder.on_element(v);
        if (self.complete) {
            // The message was a late arrival, resend result
            let result = self.builder.result().unwrap();
            debug!(self.ctx.log(), "Late-value, arrived sending new result",);
            // Report our result to the target
            self.targetPointer.tell(
                Arc::new(LocalElement {
                    data: result,
                    timestamp: self.timestamp,
                }),
                &self.actor_ref(),
            );
        }
    }

    fn handle_window_message(&mut self, msg: &WindowMessage) -> () {
        let payload = msg.payload.as_ref().unwrap();
        match payload {
            element(e) => {
                // TODO: Handle elements from remote source
                //self.add_value(e.data); <- Doesn't work because element isn't generic
            }
            trigger(t) => {
                self.trigger();
            }
            complete(c) => {
                // Suicide
                debug!(self.ctx.log(), "Window {} shutting down", self.timestamp);
                // Arc<crate::prelude::Component<TestComp>>

                //self.ctx.system().kill(Arc::new(self.core));
            }
        }
    }
    fn trigger(&mut self) -> () {
        // Close the window, only trigger if we weren't already complete
        if (!self.complete) {
            self.complete = true;
            let result = self.builder.result().unwrap();
            debug!(
                self.ctx.log(),
                "triggered result for window {}", self.timestamp
            );
            // Report our result to the target
            self.targetPointer.tell(
                Arc::new(LocalElement {
                    data: result,
                    timestamp: self.timestamp,
                }),
                &self.actor_ref(),
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
                self.trigger();
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
                error!(self.ctx.log(), "Failed to handle WindowMessage",);
            }
        } else {
            error!(
                self.ctx.log(),
                "Window {} bad remote message from {}", self.timestamp, sender
            );
        }
    }
}