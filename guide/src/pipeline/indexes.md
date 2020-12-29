# State Indexes


We will use the following ArconState object to showcase the different indexes.
```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/index.rs:state}}
```

## Value

A value index holds a single ArconType `T` and provides a set of operations to handle the value.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/index.rs:value}}
```

There is no `Eager` version of Value provided as it does not make much sense while 
dealing with a single object.

## Map

An index for managing an unordered hash index. 

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/index.rs:map}}
```

An index for appending data which then later can be consumed.

## Appender

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/index.rs:appender}}
```
