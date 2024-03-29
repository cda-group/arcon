+++
title = "About"
description = ""
date = 2021-05-01T18:20:00+00:00
updated = 2021-05-01T18:20:00+00:00
draft = false
weight = 120
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = ""
toc = true
top = false
+++

Arcon is a library for building real-time analytics applications in Rust. The Arcon runtime is based on the Dataflow model, similarly to systems such as Apache Flink and Timely Dataflow.

The Arcon philosophy is state first. Most other streaming systems are output-centric and lack a way of working with internal state with support for time semantics. Arcon’s upcoming TSS query language allows extracting and operating on state snapshots consistently based on application-time constraints and interfacing with other systems for batch and warehouse analytics.

Key features:

* Out-of-order Processing
* Event-time & Watermarks
* Epoch Snapshotting for Exactly-once Processing
* Hybrid Row(Protobuf) / Columnar (Arrow) System
* Modular State Backend Abstraction


## Project Status

We are working towards stablising the APIs and the runtime.

Arcon is in development and should be considered experimental until further notice. 

## Background

Arcon is a research project developed at KTH Royal Institute of Technology and RISE Research Institutes of Sweden in Stockholm, Sweden.