FROM rust:1.88-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY cli ./cli
COPY core/crates ./core/crates

RUN cargo build --release -p ugraph

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/ugraph /usr/local/bin/ugraph

ENTRYPOINT ["ugraph"]
