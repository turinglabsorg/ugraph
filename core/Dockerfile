FROM rust:1.88-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p ugraph

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/ugraph /usr/local/bin/ugraph
COPY examples ./examples
COPY docker/entrypoint.sh /usr/local/bin/ugraph-entrypoint
RUN chmod +x /usr/local/bin/ugraph-entrypoint

ENV UGRAPH_HOST=0.0.0.0
ENV UGRAPH_PORT=8030
ENV UGRAPH_STORAGE=json
ENV UGRAPH_STATE_FILE=/data/state.json
ENV UGRAPH_MANIFEST=/app/examples/growfi/subgraph.yaml
ENV UGRAPH_POLL_INTERVAL_MS=1000
ENV UGRAPH_RETRY_MAX_MS=60000
ENV UGRAPH_REORG_POLICY=rollback
ENV UGRAPH_REORG_CHECK_DEPTH=64
ENV UGRAPH_HISTORY_LIMIT=1024
ENV UGRAPH_MAX_BLOCK_RANGE=2000
ENV UGRAPH_RPC_RETRIES=3
ENV UGRAPH_IPFS_GATEWAY=https://ipfs.io/ipfs/
ENV UGRAPH_IPFS_TIMEOUT_SECS=60
ENV UGRAPH_MAX_IPFS_FILE_BYTES=26214400

EXPOSE 8030
ENTRYPOINT ["/usr/local/bin/ugraph-entrypoint"]
