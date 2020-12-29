# Getting Started

## Setting up Rust

The easiest way to install rust is to use the [rustup](https://rustup.rs/) tool.

Arcon currently only works on Rust nightly. See the current toolchain that is used [here](https://github.com/cda-group/arcon/blob/master/rust-toolchain).

## Cargo

Add Arcon to your cargo project as a dependency:

```toml
[dependencies]
arcon = "LATEST_VERSION"
```
The latest version can be found on [crates.io](https://crates.io/crates/arcon).

### Github Dependency

You may also add Arcon as a git dependency through Cargo. The following will use the latest master version

```toml
[dependencies]
arcon = { git = "https://github.com/cda-group/arcon" }
```

## Basic Example

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../examples/src/bin/collection.rs}}
```
