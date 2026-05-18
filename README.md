# ugraph

`ugraph` is split into two layers:

- `core/`: Rust Graph Node/Goldsky-compatible subgraph runtime and CLI.
- `infra/`: container and serverless deployment layer for running `core` online.

The storage target is Postgres. SQLite can be used for local development and
tests. Redis is out of scope for now.

## Core

```bash
cd core
cargo test
cargo run -p ugraph -- doctor --manifest examples/growfi/subgraph.yaml
```

## Infra

`infra/` will own Docker, Cloud Run, managed Postgres wiring, secrets, and the
GraphQL/GraphiQL service surface.
