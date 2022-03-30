+++
title = "Data Types"
description = "Arcon Data Types"
date = 2022-03-29T08:20:00+00:00
updated = 2022-03-29T08:20:00+00:00
draft = false
weight = 20
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = "This section covers Arcon's data types"
toc = true
top = false
+++


## Overview

Arcon is a hybrid row and columnar system. 

|Type | Format | Usage | Crate | 
|---|---| --- | --- | 
|Row | Protobuf | Ingestion / OLTP | [prost](https://github.com/tokio-rs/prost) |
|Columnar | Arrow/Parquet | Warehousing / Window analytics | [arrow](https://github.com/apache/arrow-rs) |

## Declaring an Arcon Type

Types inside the Arcon runtime must implement the [ArconType](https://docs.rs/arcon/latest/arcon/prelude/trait.ArconType.html) trait. To define an ArconType, one can use the [Arcon](https://docs.rs/arcon/latest/arcon/derive.Arcon.html) derive macro. 


```rust
use arcon::prelude::*;

#[arcon::proto]
#[derive(Arcon, Copy, Clone)]
pub struct Event {
    id: u64,
    data: f32,
    timestamp: u64,
}
```

The [arcon::proto](https://docs.rs/arcon/latest/arcon/attr.proto.html) attribute macro helps remove some boilerplate annotations required by the ``prost`` crate. Note that the Arcon derive macro needs ``prost`` to be in scope. Make sure it's listed as a dependency:

```toml
[dependencies]
arcon = "0.2"
prost = "0.9"
```


## Enabling Arrow

Data types within Arcon are not required to support Arrow conversion. However, the [Arrow](https://docs.rs/arcon/latest/arcon/derive.Arrow.html) derive macro signals that it's an Arrow type to the runtime.
Arrow analytics is limited as of now. You can follow the progress [here](https://github.com/cda-group/arcon/issues/285).

```rust
use arcon::prelude::*;

#[arcon::proto]
#[derive(Arcon, Arrow, Copy, Clone)]
pub struct ArrowEvent {
    id: u64,
    data: f32,
}
```