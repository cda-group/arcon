[![Build Status](https://dev.azure.com/arcon-cda/arcon/_apis/build/status/cda-group.arcon?branchName=master)](https://dev.azure.com/arcon-cda/arcon/_build/latest?definitionId=1&branchName=master)
[![Cargo](https://img.shields.io/badge/crates.io-v0.1.3-orange)](https://crates.io/crates/arcon)
[![License](https://img.shields.io/badge/License-AGPL--3.0--only-blue)](https://github.com/cda-group/arcon)

# arcon

This crate is not intended to be used directly. It is a part of the [Arcon project](https://github.com/cda-group/arcon)

* [`arcon_error`]: Common error utilities.
* [`arcon_extra`]: Contains extra 3rd party features.
* [`arcon_macros`]: Internal Rust macros used by Arcon.

[`arcon_error`]: arcon_error
[`arcon_extra`]: arcon_extra
[`arcon_macros`]: arcon_macros

## Criterion Benchmarks

Run All
```
$ cargo bench
```

Run specific benchmark
```
$ cargo bench --bench <bench-name>
```
