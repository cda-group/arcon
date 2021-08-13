#!/usr/bin/env bash

set -o xtrace
set -e

cargo test --all --all-features
cargo check --benches --all --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets  -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
