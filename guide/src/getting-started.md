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

### Github master

You can also point cargo to the latest [Github](https://github.com/cda-group/arcon) master version, instead of a release.
To do so add the following to your Cargo.toml instead:

```toml
[dependencies]
arcon = { git = "https://github.com/cda-group/arcon" }
```


## Basic Example

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../examples/src/bin/collection.rs}}
```
