<p align="center">
  <img width="300" height="300" src=".github/arcon_logo.png">
</p>

# Arcon

A runtime for writing real-time analytics applications in the Rust programming language.

![ci](https://github.com/cda-group/arcon/workflows/ci/badge.svg)
[![License](https://img.shields.io/badge/License-AGPL--3.0--only-blue)](https://github.com/cda-group/arcon)

#### Project Status

Arcon is in development and should be considered experimental until further notice.

#### Rust Version

Arcon builds against the latest stable release and the current MSRV is 1.53.0.

### Roadmap

See the roadmap [here](https://github.com/cda-group/arcon/projects/1)

## Highlights

*   Arrow-native
*   Hybrid Row(Protobuf) / Columnar (Arrow) System
*   Dynamic & Scalable [Middleware](https://github.com/kompics/kompact)
*   Flexible State Management
    *   Backend per Operator (e.g., RocksDB, Sled)
    *   Eager and Lazy state indexes

## Vision

<p align="center">
  <img width="600" height="300" src=".github/arcon_vision.png">
</p>

## Example

```rust,no_run
use arcon::prelude::*;

fn main() {
    let mut app = Application::default()
        .iterator(0u64..100, |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .filter(|x| *x > 50)
        .to_console()
        .build();

    app.start();
    app.await_termination();
}
```

More advanced examples can be found [here](arcon_examples/src/bin).

## Project Layout

* [`arcon_allocator`]: Custom allocator.
* [`arcon_build`]: Protobuf builder
* [`arcon_examples`]: Example Applications
* [`arcon_macros`]: Arcon derive macros.
* [`arcon_shell`]: Explore a live Arcon application
* [`arcon_state`]: State management features.
* [`arcon_tests`]: Integration tests
* [`benches`]: Criterion benchmarks
* [`src`]: Core arcon crate
* [`website`]: Project website

[`arcon_allocator`]: arcon_allocator
[`arcon_build`]: arcon_build
[`arcon_examples`]: arcon_examples
[`arcon_macros`]: arcon_macros
[`arcon_shell`]: arcon_shell
[`arcon_state`]: arcon_state
[`arcon_tests`]: arcon_tests
[`benches`]: benches
[`src`]: src
[`website`]: website

## License

This project is licensed under the [AGPL-3.0 license](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Arcon by you shall be licensed as AGPL-3.0, without any additional terms or conditions.
