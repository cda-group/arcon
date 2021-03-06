# Sources

This section will cover the data sources of Arcon. 
Sources are defined through Arcon's Pipeline object. Either by specifying a pre-defined one 
or adding your own `Source` implementation.

* [Source Configuration](#source-configuration)
* [Pre-defined Sources](#pre-defined-sources)
* [Creating a custom Source](#creating-a-custom-source)

## Source Configuration

Whether you are using a pre-defined source or your own. You will have the chance
to configure a ``SourceConf`` object. Time in Arcon is by default based on event time.
However, if you wish to swap to Process Time, you may do so by choosing ``ArconTime::Process``.
Note that if you are using ``ArconTime::Event``, then a Timestamp Extractor also needs to be set. See down 
below that shows an example of a Source that is working with the primitive ``u64``.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/api.rs:source_conf}}
```

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
