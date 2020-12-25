# State Management

In this section, we will cover how arcon approaches state.

Arcon makes a clear separation between active and historical state. Active state is maintained in in-memory indexes, while cold state is pushed down to a durable state backend. Other streaming systems commonly operate directly on the latter (e.g., RocksDB). This has several drawbacks. Typically the state backends are general-purpose key-value stores and are thus not specialised for streaming workloads. 
The state access pattern is not considered at all. Secondly, state calls have to serialise/deserialise for each operation.

However, for some workloads it may make sense to directly use the underlying state backend. Thus, each index implementation in Arcon comes with an **Eager** version. So for example,
`Map` is by default lazy while `EagerMap` operates directly on the state backend.

## Declaring Arcon State

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/stateful.rs:data}}
```

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/stateful.rs:state}}
```

