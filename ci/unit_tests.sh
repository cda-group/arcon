#!/bin/bash
set -e

NIGHTLY_VER="nightly-2019-07-04"

curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $NIGHTLY_VER
export PATH=$PATH:$HOME/.cargo/bin
rustup toolchain install $NIGHTLY_VER
rustup default $NIGHTLY_VER
rustc --version
./ci/install_protobuf.sh
./ci/install_capnproto.sh
./ci/install_llvm.sh
export RUST_BACKTRACE=1
cd arcc && cargo +$NIGHTLY_VER test --all
#cd ../ && ./ci/install_sbt.sh
#cd operational-plane && sbt test
