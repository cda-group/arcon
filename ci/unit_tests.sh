#!/bin/bash
set -e

curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain nightly
export PATH=$PATH:$HOME/.cargo/bin
rustc --version
./ci/install_protobuf.sh
./ci/install_capnproto.sh
./ci/install_llvm.sh
export RUST_BACKTRACE=1
cd arcc && cargo +nightly test --all
cd ../ && ./ci/install_sbt.sh
cd operational-plane && sbt test
