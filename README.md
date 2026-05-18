# ugraph

`ugraph` is split into two layers:

- `core/`: Rust Graph Node/Goldsky-compatible subgraph runtime and CLI.
- `infra/`: container and serverless deployment layer for running `core` online.

The storage target is Postgres. SQLite can be used for local development and
tests. Redis is out of scope for now.

## Production Shape

Production should use a shared multi-chain feed, not one RPC scanner per
subgraph:

- One `chain-reader` per `chain_id` reads raw blocks/logs from that chain's
  RPC.
- The reader stores raw chain data in Postgres.
- Each subgraph deployment consumes matching raw logs from that feed and writes
  its own entity store under a separate deployment id.
- Multiple subgraphs and versions can share the same Postgres instance.
  Deployments on the same chain share a reader; deployments on different chains
  use separate readers keyed by `chain_id`.
- Multi-chain subgraphs are represented as one deployment with multiple
  chain-scoped subscriptions.

The intended operator flow is a single CLI command such as:

```bash
ugraph deploy --provider local --deployment growfi-v1 --chain-id 11155111 --manifest subgraph.yaml --postgres-url <postgres-url> --rpc-url <rpc>
```

That command should create or reuse the shared infrastructure, ensure readers
exist for the required chains, register the deployment, run sync, and expose
GraphQL/GraphiQL.

The current implementation supports the local version of that flow. The same
Docker image can run as `serve`, `indexer`, or `chain-reader`; `docker compose`
starts Postgres, the shared reader, the feed-backed indexer, and the API.
Local `deploy` loops bounded reader/sync passes so dynamic data sources created
by mappings are subscribed, backfilled, and indexed before the command reports
success.

Deployment names are unique inside one Postgres-backed instance. The public API
serves the current deployment through Graph Node/Goldsky-style paths:
`/subgraphs/<deployment>/<version>/gn`,
`/subgraphs/<deployment>/<version>/graphql`, and the `latest` alias. Explicit
version paths are accepted only when they match registered deployment metadata.

## Core

```bash
cd core
cargo test
cargo run -p ugraph -- doctor --manifest examples/growfi/subgraph.yaml
```

## Infra

`infra/` owns online deployment wiring. The lowest-cost target is currently
`infra/gcp/e2-micro`: one Google Compute Engine `e2-micro` VM, local Docker
Compose, local Postgres, and direct image upload without Cloud SQL, Cloud Run,
or Artifact Registry. The public edge is Caddy HTTPS on either a custom domain
such as `ugraph.growfi.dev` or the generated `<external-ip>.sslip.io` hostname;
the API and Postgres stay internal to the Docker network.
