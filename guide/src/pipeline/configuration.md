# Configuration

Arcon uses HOCON for its configuration.

Full list of possible configurations may be found [here](https://github.com/cda-group/arcon/blob/master/src/conf/mod.rs).

## Example Configuration file

This configuration file changes the interval for both watermarks and epochs, but also changes
the batching of events to a maximum of 1024 events.

All other configuration parameters are set to their default values.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/arcon.conf}}
```

## Loading Configuration from file

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/configuration.rs}}
```
