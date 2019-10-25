[![Build Status](https://dev.azure.com/arcon-cda/arcon/_apis/build/status/cda-group.arcon?branchName=master)](https://dev.azure.com/arcon-cda/arcon/_build/latest?definitionId=1&branchName=master)
[![Cargo](https://img.shields.io/badge/crates.io-v0.1.1-orange)](https://crates.io/crates/arcon_compiler)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue)](https://github.com/cda-group/arcon)

# Arcon Compiler

Installing:

```bash
$ cargo +nightly install arcon_compiler
```

**NOTE**: requires Protobuf. Instructions can be found [here](https://github.com/cda-group/arcon/tree/master/execution-plane)

## Building

```
$ cargo build --release
```

## Compile Arcon Application


```
$ ./target/release/arconc compile -s "path_to_arcon_spec"
```


## Server

```
$ ./target/release/arconc server # defaults to localhost:3000
```

## Flags

```
$ ./target/release/arconc help
```
