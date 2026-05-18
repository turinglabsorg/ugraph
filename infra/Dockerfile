FROM rust:1.88-bookworm AS builder

WORKDIR /app/core
COPY core/Cargo.toml core/Cargo.lock ./
COPY core/crates ./crates

RUN cargo build --release -p ugraph

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/core/target/release/ugraph /usr/local/bin/ugraph

ENTRYPOINT ["ugraph"]
