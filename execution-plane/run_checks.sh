#!/usr/bin/env bash

set -o xtrace
set -e

cargo test --all --all-features
cargo check --benches --all --all-features
