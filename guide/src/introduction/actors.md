# Actors

The Actor model is an old concept introduced by Carl Hewitt in 1973, but it only really has become popular with the Erlang language, as well as the Akka framework. In this model, the term for a *light-weight process with internal state* is "an actor", though we will stick to calling the equivalents in Kompact a "component" to avoid having two names for the same thing.

### Messages and References

Actors communicate via *messages*, which really are the same things as *events*, except they are addressed to a particular actor. This addressing is done via a concept called *actor reference*, which is a shareable datastruct that identifies an actor in a way that messages can be sent directly to it. In Erlang this is implemented as a "pid", while in actor there is a class `ActorRef`. Kompact also has a struct called `ActorRef`, which fulfills the same purpose on a local Kompact system. However, as we will discuss later in more details, Kompact explicitly differentiates possible remote actors at the type level. References to them are instances of `ActorPath` instead of `ActorRef`.

In Kompact, actors are statically typed with respect to the messages they can receive. That is, whenever you are implementing the `Actor` trait in Kompact for a component, you **must** specify a concrete `Message` type (as an associated type). Consequently, references to actors are also typed, so senders can only send valid messages to an actor. Thus a Kompact component which implements `Actor` with `type Message = M;` for some type `M` is referenced locally by an `ActorRef<M>`. The same is not true for `ActorPath`, as it is generally not possible to control what things are sent over a network, and so networked actors may have to deal with a wider range of possible incoming messages.

### Actors and Components

In Kompact every component is an actor and vice versa. Both the `Actor` (actually `ActorRaw`) and the `ComponentDefinition` trait need to be implemented in either case. But as we saw in the "Hello World"-example, the `Actor` trait can simply be derived when it's not used by a component:

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/helloworld.rs:declaration}}
```

The derived code will produce an actor implementation with `type Message = Never;`, indicating that no local messages can be sent to it. However, network messages still can, but will simply be discarded. This avoids a common issue encountered in Erlang, where unhandled messages keep queuing up on ports forever.

If, say, we wanted to implement an actor variant of the "Hello World"-example, we could do so by implementing the `Actor` trait ourselves with some trivial type (e.g., the unit type `()`), as in:

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/actor_helloworld.rs:actor}}
```

Of course, a unit message is not going to be produced by the Kompact runtime as a lifecycle event, so we must send it to our component after creating it, using the `tell(...)` function:

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/actor_helloworld.rs:main}}
```

Just to point out some of the particularities described above, we have annotated some types in the previous example. You can see that our `HelloWorldActor` is still created as an `Arc<Component<HelloWorldActor>>`, and also that the actor reference it produces is appropriately typed as `ActorRef<()>`.

> **Note:** As before, if you have checked out the [examples folder](https://github.com/kompics/kompact/tree/master/docs/examples) you can run the concrete binary with:
> ```bash
> cargo run --release --bin actor_helloworld
> ```
