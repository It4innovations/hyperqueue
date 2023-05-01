FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
WORKDIR /app

FROM chef as planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
WORKDIR /build
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies and cache them in a Docker layer
RUN cargo chef cook --release --recipe-path recipe.json

# Build HyperQueue itself
COPY . .
RUN cargo build --release

FROM ubuntu:22.04 AS runtime

WORKDIR /
COPY --from=builder /build/target/release/hq hq

ENTRYPOINT ["./hq"]
