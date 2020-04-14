# Execution Plane

Requires Rust nightly (See current toolchain [here](rust-toolchain)).

* [`arcon`]: Crate containing the core execution engine
* [`arcon_codegen`]: Code generation for Arcon
* [`arcon_compiler`]: Backend compiler targeting Arcon 

[`arcon`]: arcon
[`arcon_codegen`]: arcon_codegen
[`arcon_compiler`]: arcon_compiler


## Ubuntu

Install Protobuf:

```bash
  $ wget https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip
  $ unzip protoc-3.6.1-linux-x86_64.zip -d protoc3
  $ sudo mv protoc3/bin/* /usr/local/bin/
  $ sudo mv protoc3/include/* /usr/local/include/
```
## MacOS

Install Protobuf:
  
```bash
  $ brew install protobuf@3.6
  $ sudo cp /usr/local/opt/protobuf@3.6/bin/protoc /usr/local/bin/
  $ sudo cp /usr/local/opt/protobuf@3.6/include/* /usr/local/include/
```


## Other Requirements

Depending on what you are compiling, you might need cmake, g++ etc...

## Building

```
$ cargo build --release
```

## Running an Example (NOT WORKING AT THE MOMENT)

Compile and start binary:

```bash
$ ./target/release/arconc -l compile -s arcon_codegen/tests/specifications/basic_dataflow.json
$ ./build/target/release/basic_dataflow
```

Connect to Socket Source and enter valid unsigned integers:

```bash
$ nc -u localhost 5000
```

## Testing

General
```
$ cargo test --all
```

Specific test case
```
$ cargo test <test-name>
```
