# Data Format

The main data format of Arcon is Protobuf. There are several reasons behind this choice and they are listed below:

1.  Universal data format that enables the data to be moved outside of the Rust ecosystem. 
2.  Supports schema evolution.
3.  Good space utilisation on disk.
4.  Decent serialisation/deserialisation cost.


Serialised size of two different Rust structs ([Reference](https://github.com/cda-group/arcon/blob/master/benches/serde.rs)):

| Framework       |  Small |  Large |
| ------------- |-------------| -----|
| Protobuf  | 14 bytes   |   106 bytes
| Serde::Bincode   | 20 bytes | 228 bytes

Arcon uses the [prost](https://github.com/danburkert/prost) crate to define its data types directly in Rust or through .proto files.

## ArconType

Data that is passed through the runtime must implement `ArconType`. There are a few mandatory attributes
that must be added:

*   `version` An integer representing the version
*   `reliable_ser_id` An integer representing the version used for in-flight serialisation
*   `keys` fields that should be used to hash the key
    *   If not specified, it will pick all hashable fields.
    *   The Arcon derive macro will estimate the amount of bytes for the selected fields
        and pick a suitable hasher.
*   `unsafe_ser_id` An integer representing the unsafe in-flight serde if the **unsafe_flight** feature is enabled.



## Declaring Data directly in Rust

First make sure that you have also added prost as a dependency.

```toml
[dependencies]
arcon = "LATEST_VERSION"
prost = "ARCON_PROST_VERSION"
```

Then you can directly declare ArconTypes by using the `Arcon` derive macro together with the `Prost` macro:

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/stateful.rs:data}}
```
## Generating from .proto files

Down below is an example of the same `Event` struct as defined above. Note that
the mandatory attributes are gathered through regular `//` comments.

```proto
{{#rustdoc_include ../../examples/src/event.proto}}
```

We then need to use the `arcon_build` crate for generating the .proto files into Arcon supported data.

```toml
[build-dependencies]
arcon_build = "ARCON_VERSION"
```

Add a `build.rs` file and change the paths if necessary.

```rust
{{#rustdoc_include ../../examples/build.rs}}
```

The build script will then generate the .rs files and you can
include the generated data into an Arcon application by doing the following:
```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/arcon_build.rs}}
```


