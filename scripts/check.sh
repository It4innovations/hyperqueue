#!/bin/bash

set -e

cd `dirname $0`/..

# Format Rust code
cargo fmt --all

# Format Python code
isort --profile black tests benchmarks
black tests benchmarks

# Lint Python code
flake8 tests benchmarks

# Run Rust tests
cargo test

# Run Rust linter
cargo clippy --all -- -D warnings
cargo check --all --all-targets

# Build Rust binaries
cargo build --all

# Run Python tests
python -m pytest tests -n32
