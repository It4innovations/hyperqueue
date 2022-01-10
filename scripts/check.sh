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

# Test Rust code
cargo test

# Build Rust binaries
cargo build --all

# Test Python code
python -m pytest tests -n32

# Lint Rust code
cargo clippy --all -- -D warnings
cargo check --all --all-targets
