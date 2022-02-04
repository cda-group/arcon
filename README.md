<p align="center">
  <img width="300" height="300" src=".github/arcon_logo.png">
</p>

# Arcon

Arcon is a library for building state-first streaming applications in Rust.

![ci](https://github.com/cda-group/arcon/workflows/ci/badge.svg)
[![Cargo](https://img.shields.io/badge/crates.io-v0.2.0-orange)](https://crates.io/crates/arcon)
[![Documentation](https://docs.rs/arcon/badge.svg)](https://docs.rs/arcon)
[![project chat](https://img.shields.io/badge/zulip-join%20chat-ff69b4)](https://arcon.zulipchat.com)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/cda-group/arcon)

#### Project Status

Arcon is in development and should be considered experimental until further notice.

The APIs may break and you should not be running Arcon with important data!

#### Rust Version

Arcon builds against the latest stable release and the current MSRV is 1.56.1

### Roadmap

See the roadmap [here](https://github.com/cda-group/arcon/projects/1)

## Highlights

* Out-of-order Processing
* Event-time & Watermarks
* Epoch Snapshotting for Exactly-once Processing
* Hybrid Row(Protobuf) / Columnar (Arrow) System
* Modular State Backend Abstraction

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

More examples can be found [here](examples).

## Project Layout

* [`arcon`]: Arcon crate
* [`arcon_build`]: Protobuf builder
* [`arcon_macros`]: Arcon derive macros.
* [`arcon_tests`]: Integration tests
* [`arcon_util`]: Common Arcon utilities
* [`examples`]: Example applications
* [`website`]: Project website

[`arcon`]: arcon
[`arcon_build`]: arcon_build
[`arcon_macros`]: arcon_macros
[`arcon_tests`]: arcon_tests
[`arcon_util`]: arcon_util
[`examples`]: examples
[`website`]: website

## Contributing

See [Contributing](CONTRIBUTING.md)

## Community

Arcon is an ambitious project with many different development & research areas.

If you find Arcon interesting and want to learn more, then join the [Zulip](https://arcon.zulipchat.com) community!

## Acknowledgements

Arcon is influenced by many great projects whether it is implementation, code practices or project structure:

- [Tokio](https://github.com/tokio-rs/tokio)
- [Datafusion](https://github.com/apache/arrow-datafusion)
- [Apache Flink](https://github.com/apache/flink)
- [Sled](https://github.com/spacejam/sled)

## License

This project is licensed under the [Apache-2.0 license](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Arcon by you shall be licensed as Apache-2.0, without any additional terms or conditions.