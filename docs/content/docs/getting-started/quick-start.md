+++
title = "Quick Start"
description = "Arcon Quick Start"
date = 2021-05-01T08:20:00+00:00
updated = 2021-05-01T08:20:00+00:00
draft = false
weight = 10
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = "This section covers the steps of getting started with Arcon."
toc = true
top = false
+++

# Setting up Rust

The easiest way to install Rust is to use the [rustup](https://rustup.rs/) tool.

# Cargo

Add Arcon as a dependency in your ``Cargo.toml``. Note that we also include [prost](https://github.com/tokio-rs/prost) as it is required when declaring more complex Arcon data types.

```toml
[dependencies]
arcon = "0.2"
prost = "0.9"
```

# Defining an Arcon application

```rust
#[arcon::app]
fn main() {
    (0..10u64)
        .to_stream(|conf| conf.set_arcon_time(ArconTime::Process))
        .filter(|x| *x > 50)
        .map(|x| x * 10)
        .print()
}
```