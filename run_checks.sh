#!/usr/bin/env bash

set -o xtrace
set -e

cargo hack test --each-feature
cargo check --benches --all --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings