[![Build Status](https://dev.azure.com/arcon-cda/arcon/_apis/build/status/cda-group.arcon?branchName=master)](https://dev.azure.com/arcon-cda/arcon/_build/latest?definitionId=1&branchName=master)
[![License](https://img.shields.io/badge/License-AGPL--3.0--only-blue)](https://github.com/cda-group/arcon)

# Arcon

Arcon is a streaming-first execution engine for the [Arc](https://github.com/cda-group/arc) language.

## Overview

**Note**: The project is still in an early development stage.

<p align="center">
  <img width="600" height="300" src=".github/arcon_overview.jpg">
</p>


## Project Layout

* [`execution-plane`]: The execution plane provides a Rust-based distributed dataflow runtime that executes Arc applications.
* [`operational-plane`]: The operational plane is responsible for the coordination of the distributed execution of an Arc application.
* [`protobuf`]: Protobuf messages used between Arcon's operational and execution plane.

[`execution-plane`]: execution-plane
[`operational-plane`]: operational-plane
[`protobuf`]: protobuf

## Related Projects

* [Arc](https://github.com/cda-group/arc): An IR and compiler for data analytics.
* [Kompact](https://github.com/kompics/kompact): A hybrid Actor + Component model framework.
