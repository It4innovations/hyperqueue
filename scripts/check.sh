#!/bin/bash

set -e

cd `dirname $0`/..

# Format Rust code
cargo fmt

# Format Python tests
isort --profile black tests
black tests

# Lint Python tests
flake8 tests

# Run Rust tests
cargo test

# Run Rust linter
cargo clippy

# Build Rust binaries
cargo build

# Run Python tests
python -m pytest tests -n32
