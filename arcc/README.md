# arcc

arcc is the Arc backend compiler targeting the Arcon runtime.

Install Rust (Requires Nightly):


```bash
$ curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain nightly
```

## Ubuntu (>= 16.04)

Install LLVM:

```bash
$ wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
$ sudo apt-add-repository "deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-6.0 main"
$ sudo apt update && sudo apt install clang-6.0
$ sudo ln -s /usr/bin/llvm-config-6.0 /usr/local/bin/llvm-config
$ sudo apt-get install -y zlib1g-dev
```

Install Protobuf:

```bash
  $ wget https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip
  $ unzip protoc-3.6.1-linux-x86_64.zip -d protoc3
  $ sudo mv protoc3/bin/* /usr/local/bin/
  $ sudo mv protoc3/include/* /usr/local/include/
```
## MacOS

Install LLVM:
  
```bash
  $ brew install llvm@6
  $ sudo ln -s /usr/local/Cellar/llvm@6/6.0.1_1/bin/llvm-config /usr/local/bin/llvm-config
```

Install Protobuf:
  
```bash
  $ brew install protobuf@3.6
  $ sudo cp /usr/local/opt/protobuf@3.6/bin/protoc /usr/local/bin/
  $ sudo cp /usr/local/opt/protobuf@3.6/include/* /usr/local/include/
```

## Building

```
$ cargo build --release
```

## Running an Example

Compile and start binary:

```bash
$ ./target/release/arcc compile -s codegen/tests/specifications/basic_dataflow.json
$ ./build/target/release/basic_dataflow
```

Connect to Socket Source and enter valid unsigned integers:

```bash
$ nc localhost 1337
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
