#!/bin/bash

set -e

cd `dirname $0`/..

# Format Rust code
cargo fmt

# Format Python code
isort --profile black tests benchmarks
black tests benchmarks

# Lint Python code
flake8 tests benchmarks

# Run Rust tests
cargo test

# Run Rust linter
cargo clippy -- -D warnings
cargo check --all-targets

# Build Rust binaries
cargo build

# Run Python tests
python -m pytest tests -n32
