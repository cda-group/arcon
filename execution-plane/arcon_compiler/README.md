[![Build Status](https://dev.azure.com/arcon-cda/arcon/_apis/build/status/cda-group.arcon?branchName=master)](https://dev.azure.com/arcon-cda/arcon/_build/latest?definitionId=1&branchName=master)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue)](https://github.com/cda-group/arcon)

# Arcon Compiler

`arconc` takes an `ArconSpec` and compiles it into a Rust binary.


## Building

```
$ cargo build --release
```

## Compile


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
