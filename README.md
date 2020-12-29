# Arcon

A runtime for writing streaming applications with the Rust programming language.

![ci](https://github.com/cda-group/arcon/workflows/ci/badge.svg)
[![Cargo](https://img.shields.io/badge/crates.io-v0.1.3-orange)](https://crates.io/crates/arcon)
[![License](https://img.shields.io/badge/License-AGPL--3.0--only-blue)](https://github.com/cda-group/arcon)

#### Project Status

Arcon is in development and should be considered experimental until further notice.

#### Rust Version

Arcon currently requires Rust nightly (See current toolchain [here](rust-toolchain)).

#### User Guide

More detailed information about Arcon can be found [here](https://cda-group.github.io/arcon)

## Example

```rust,no_run
use arcon::prelude::*;

fn main() {
    let mut pipeline = Pipeline::default()
        .collection((0..100).collect::<Vec<u64>>(), |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .filter(|x| *x > 50)
        .map(|x| x + 10)
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
```

More advanced examples can be found [here](guide/examples).

## Project Layout

* [`arcon_allocator`]: Custom allocator.
* [`arcon_build`]: Protobuf builder
* [`arcon_error`]: Common error utilities.
* [`arcon_macros`]: Arcon derive macros.
* [`arcon_state`]: State management features.
* [`arcon_tests`]: Integration tests
* [`arcon_tui`]: Text-based dashboard.

[`arcon_allocator`]: arcon_allocator
[`arcon_build`]: arcon_build
[`arcon_error`]: arcon_error
[`arcon_macros`]: arcon_macros
[`arcon_state`]: arcon_state
[`arcon_tests`]: arcon_tests
[`arcon_tui`]: arcon_tui

## License

This project is licensed under the [AGPL-3.0 license](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Arcon by you shall be licensed as AGPL-3.0, without any additional terms or conditions.
