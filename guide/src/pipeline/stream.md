# Stream API

A `Stream` in Arcon represents an unbounded or bounded stream of data. 

Users may add a transformation on one Stream to produce another. The sequence of transformations
is added to a dataflow graph which is then later used to build the final Pipeline.

Streams are initially created from Arcon sources.

The section is split into the follow sub-sections: 

- [Operator Builder](#operator-builder)
- [Pre-defined Operators](#pre-defined-operators)
- [Running a Pipeline](#running-a-pipeline)
- [Basic Pipeline](#basic-pipeline)

## Operator Builder

An Operator Builder is used to define an Operator in Arcon pipeline. The builder requires two fields, a constructor and 
configuration.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/api.rs:operator}}
```
### Operator Constructor

The constructor of the builder has access to a ``Backend`` object. If your operator is stateless, you may simply
ignore it as it has been done above. If the ``rocksdb`` feature is enabled, then Rocks will be used as the default ``Backend``,
otherwise Arcon defaults to ``Sled``. If you wish to configure a specific Backend, then simply annotate it with ``|backend: Arc<Rocks>|``.

### Operator Configuration

Currently the configuration has two possible parameters. (1) ParallelismStrategy, where you may configure a static one ``ParallelismStrategy::Static(usize)``
or managed ``ParallelismStrategy::Managed``. Note that only the Static strategy works for now; (2) StreamKind defines the type of Stream
we are dealing with. Arcon is for now only focusing on Keyed streams and therefore the default StreamKind is ``StreamKind::Keyed``.


You may configure a default operator configuration through ``conf: Default::default()``.

## Pre-defined Operators

Arcon comes with a set of high-level function operators. All of the operators can either be stateless using 
the ``new`` constructor or stateful through``stateful``.  In the latter, you will need to pass both the User-defined Function and
an ``ArconState`` object.

### Map

An Operator that maps ``IN: ArconType`` to ``OUT: ArconType``.

```rust,edition2018,no_run,noplaypen
Map::new(|x: u64| x + 10)
```

### MapInPlace

An Operator that maps in-place on ``IN: ArconType`` and outputs the same type.

```rust,edition2018,no_run,noplaypen
MapInPlace::new(|x: &mut u64| *x + 10)
```

### Filter

An Operator that filters away ``IN: ArconType`` based on a predicate.

```rust,edition2018,no_run,noplaypen
Filter::new(|x: &u64| *x > 10)
```

### Flatmap

An Operator that accepts a single ``IN: ArconType`` and outputs N elements of ``OUT: ArconType``.
The expected output type of the function is an ``IntoIterator`` where each Item is of type ``OUT``.

```rust,edition2018,no_run,noplaypen
Flatmap::new(|x: u64| (0..x))
```

## Running a Pipeline

Once you are done with adding transformations and want to build the pipeline, the ``build()`` function on a Stream type
will construct the Dataflow components and return an ``AssembledPipeline``.
Arcon does not require any explicit sink. If you wish to debug the output of the last Operator, then see the ``to_console()`` function.

Note that an ``AssembledPipeline`` does not start processing data until it has been started.

## Basic Pipeline

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/collection.rs}}
```
