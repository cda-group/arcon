# Components

In the Kompics component model, the term for a *light-weight process with internal state* is a "component".
This notion can be further subdivided into the process-part of a component, a *component core*, and the state-part of a component, which is called a *component definition*. The *core* basically just interacts with the runtime of the system, while the *definition* contains the state variables and behaviours, in the form of *ports* and *event handlers*, which we will discuss below.

The execution model of a component model always ensures that the state variables of a component definition can be accessed safely without any synchronisation.

In the Kompact implementation, a *component definition* is simply a Rust struct that contains a `ComponentContext` as a field and implements the `ComponentDefinition` trait, which is typically just derived automatically, as we saw in the "Hello World"-example:

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/helloworld.rs:declaration}}
```

The *component core* itself is hidden from us in Kompact, but we can interact with it using the `ComponentContext` field from within a component. When we actually instantiate a component as part of a Kompact system, we are given an `Arc<Component>`, which is a combined reference to the component definition and core. The creation of this structure is what really happened when we invoked `system.create(...)` in the "Hello World"-example:

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/helloworld.rs:create}}
```

## Events and Ports

Components communicate via *events* that are propagated along channels to a set of target components. The Kompics model is very strict about which events may travel on which channels, which it formalises with the concept of a *port*. Ports basically just state which events may travel in which way through the channels connected to it. You can think of a port as an API specification. If a component *C* *provides* the API of a port *P*, then it will accept all events of types that are marked as *request* in *P*, and it will only send events of the types that are marked as *indication* in *P*. Since components communicate with each other, the dual notion to providing a port is *requiring* it, and channels may only connect opposite variants of ports. That is, if one end of a channel is connected to a *provided* port of type *P* then the other side **must** be connected to a *required* port of type *P*. This setup ensures that messages which are sent through the channel are also accepted on the other side.

In Kompact each port is limited to a single *indication* and a single *request* type. If more types are needed in either direction, they must be wrapped into an enum, which is facilitated easily in Rust using the `From` and `Into` traits. 

For example, a simplified version of Kompact's internal `ControlPort`, which could be defined something like this:
```rust,edition2018,no_run,noplaypen
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlEvent {
    Start,
    Stop,
    Kill,
}

pub struct ControlPort;

impl Port for ControlPort {
    type Indication = Never; // alias for the ! bottom type
    type Request = ControlEvent;
}
```
It has a single *request* event of type `ControlEvent`, which provides three different variants, invoked at particular points in a component's lifecycle. It does not send any *indication* events, however, which is marked by the `Never` type, which is uninhabited.

In order to react to the events of a port we must implement an trait appropriate trait for the direction of the events. For the control port above, for example, we might want to implement `Provide<ControlPort>` to react to `ControlEvent` instances. This could look as follows, for example:

```rust,edition2018,no_run,noplaypen
impl Provide<ControlPort> for HelloWorldComponent {
	fn handle(&mut self, event: ControlEvent) -> Handled {
		match event {
			ControlEvent::Start => {
				info!(self.log(), "Hello World!");
        		self.ctx.system().shutdown_async();
        		Handled::Ok
			}
			ControlEvent::Stop | ControlEvent::Kill => Handled::Ok,
		}
	}
}
```
This mechanism is similar to the concept of *event handlers* in the Kompics model, except that you can only have a single handler in Kompact and it is *always* (statically) *subscribed*. In this way the compiler can statically ensure that any component providing (or requiring) a port also accepts the appropriate events.

In Kompact, however, the `ControlPort` is not exposed (anymore since version `0.10.0`), but instead we must implement the `ComponentLifecycle` trait to react to (some of) its events, as we did in the `HelloWorldComponent` example:

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/helloworld.rs:lifecycle}}
```

### Channels

We haven't seen an example of channels, yet, but we will get there, when we are talking about local Kompact execution. Suffice to say here, that channels do not actually have a corresponding Rust struct in Kompact, but are simply a mental concept to think about how ports are connected to each other. Each port really just maintains a list of other ports it is connected to, and broadcasts all outgoing events to all of the connected ports. This is also why Kompact requires all events to implement the `Clone` trait. If cloning of an event for each connected component would be too expensive, it can often be a good alternative to simply share it immutably behind an `Arc`. Sharing events mutably behind an `Arc<Mutex<_>>` is of course also possible, but generally discouraged, as contention on the `Mutex` could drag down system performance significantly.

These "broadcast by default"-semantics are probably the most fundamental difference between the Kompics component model, and the Actor model we will talk about in the next section.
