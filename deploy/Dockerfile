FROM rust:1.99.1-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY proto proto
COPY crates crates
RUN cargo build --release --locked --bin shard --bin simulator

FROM debian:trixie-slim AS runtime
RUN useradd --system app
USER app

FROM runtime AS shard
COPY --from=build /src/target/release/shard /usr/local/bin/shard
ENTRYPOINT ["shard"]

FROM runtime AS simulator
COPY --from=build /src/target/release/simulator /usr/local/bin/simulator
ENTRYPOINT ["simulator"]
