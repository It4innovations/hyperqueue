#!/bin/bash

set -e

cd `dirname $0`/..

# Format Rust code
cargo fmt --all

# Format Python code
isort --profile black scripts tests benchmarks crates/pyhq/python
black scripts tests benchmarks crates/pyhq/python

# Lint Python code
flake8 scripts tests benchmarks crates/pyhq/python

# Test Rust code
cargo test

# Build Rust binaries
cargo build --all

# Test Python code
python -m pytest tests -n32

# Lint Rust code
cargo clippy --all -- -D warnings
cargo check --all --all-targets
