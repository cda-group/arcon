[![Build Status](https://dev.azure.com/arcon-cda/arcon/_apis/build/status/cda-group.arcon?branchName=master)](https://dev.azure.com/arcon-cda/arcon/_build/latest?definitionId=1&branchName=master)
[![Cargo](https://img.shields.io/badge/crates.io-v0.1.3-orange)](https://crates.io/crates/arcon)
[![License](https://img.shields.io/badge/License-AGPL--3.0--only-blue)](https://github.com/cda-group/arcon)

# arcon

A Streaming-first Analytics Engine.

Requires Rust nightly (See current toolchain [here](rust-toolchain)).

* [`arcon_allocator`]: Custom allocator.
* [`arcon_build`]: Protobuf builder
* [`arcon_error`]: Common error utilities.
* [`arcon_extra`]: Contains extra 3rd party features.
* [`arcon_macros`]: Internal Rust macros.
* [`arcon_state`]: State management features.
* [`arcon_tui`]: Text-based dashboard.

[`arcon_allocator`]: arcon_allocator
[`arcon_build`]: arcon_build
[`arcon_error`]: arcon_error
[`arcon_extra`]: arcon_extra
[`arcon_macros`]: arcon_macros
[`arcon_state`]: arcon_state
[`arcon_tui`]: arcon_tui

## Requirements

Depending on what you are compiling, you might need cmake, g++ etc...

## Testing

General
```
$ cargo test --all
```

Specific test case
```
$ cargo test <test-name>
```

## Criterion Benchmarks

Run All
```
$ cargo bench
```

Run specific benchmark
```
$ cargo bench --bench <bench-name>
```

## License

This project is licensed under the [AGPL-3.0 license](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Arcon by you shall be licensed as AGPL-3.0, without any additional terms or conditions.
