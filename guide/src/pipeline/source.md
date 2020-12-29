# Sources

This section will cover the data sources of Arcon. 
Sources are defined through Arcon's Pipeline object. Either by specifying a pre-defined one 
or adding your own `Source` implementation.

* [Source Configuration](#source-configuration)
* [Pre-defined Sources](#pre-defined-sources)
* [Creating a custom Source](#creating-a-custom-source)

## Source Configuration

Whether you are using a pre-defined source or your own. You will have access 
to modify a `SourceConf` object through a closure.

By default, `Time` in Arcon is based on event time. However if you want to configure
it to Process time, see the [File](#file) source example.


## Pre-defined Sources

### Collection

A Collection source is mostly for testing things out.
It takes a `Vec<A>` as argument where `A: ArconType`.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/collection.rs}}
```

### File

The example code reads u64s from a file called
`file_source_data`. The file source requires your input data to implement `ArconType` but also
Rust's `std::str::FromStr` type.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/file.rs}}
```

## Creating a custom Source

To be added.
