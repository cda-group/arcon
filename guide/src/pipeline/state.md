# Accessing State

This section covers different approaches to accessing state of a live Arcon application.

Arcon State is identified through a `StateID` which is an type alias
for a String. The state id is set through the Operator configuration that was discussed in the [Stream API](stream.md) section.

- [Snapshot](#snapshot)
- [Watching from a thread](#watching-from-a-thread)
- [Watching from a Kompact component](#watching-from-a-kompact-component)
- [Executing SQL queries](#executing-sql-queries)

## Snapshot

Arcon uses a epoch snapshotting protocol in order to take consistent state snapshots of an application.
Once a Snapshot has been committed, it will be available for access outside the live execution.

## State Setup

We will use the following state for this section.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/stateful.rs:state}}
```

## Watching from a thread

With the `watch` command, we can get access to some `ArconState` from our application per Epoch.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/stateful.rs:watch_thread}}
```
The above call assumes that MyState has been registered with the state id `map_state_one`.
And do note that for the watch call to work, MyState needs to implement `From<Snapshot>` as seen below.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/stateful.rs:snapshot}}
```

## Watching from a Kompact component

If you have a [Kompact](https://github.com/kompics/kompact) application that needs to act on the data from an Arcon application.
Then it is possible to register components to receive Snapshots. However it is up to the 
external component to construct the `ArconState` from the received `Snapshot`.

Example Component
```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/snapshot_component.rs}}
```
We can then add this component to receive Snapshots by executing `watch_with` which
takes a state id and and any Kompact component that implements `Message = Snapshot`.
The snippet below shows how an example of how to do this.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/stateful.rs:watch_component}}
```

## Executing SQL queries

Feature not implemented yet!
