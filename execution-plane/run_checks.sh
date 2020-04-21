#!/usr/bin/env bash

cargo test --all --all-features
cargo check --benches --all --all-features
