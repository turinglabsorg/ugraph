# ugraph

`ugraph` is split into two layers:

- `core/`: Rust Graph Protocol-compatible libraries, fixtures, docs, and
  Docker runtime assets. The production container builds `ugraph-node` from
  `core/crates/ugraph-node` and does not copy the user CLI source.
- `cli/`: the `ugraph` operator binary for local/user/admin commands.
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

The intended operator flow is a single CLI command. For a local Postgres-backed
instance:

```bash
ugraph deploy --provider local --deployment growfi-v1 --chain-id 11155111 --manifest subgraph.yaml --postgres-url <postgres-url> --rpc-url <rpc>
```

For a hosted ugraph instance:

```bash
ugraph auth login --endpoint https://ugraph.example.com --api-key <ugraph-api-key>
ugraph deploy --provider remote --deployment growfi --version 4.0.4 --visibility public --chain-id 11155111 --manifest subgraph.yaml --rpc-url <rpc>
```

The remote command uploads the compiled subgraph bundle to the server, runs
`ugraph-node sync` on the server, promotes the uploaded version, and exposes it
through `/subgraphs/<deployment>/<version>/gn` plus the `latest` alias.

The current implementation supports both the local deploy flow and the first
remote hosted flow. The core Docker image runs the node runtime modes: `serve`,
`indexer`, or `chain-reader`; the API mode also accepts authenticated remote
deploy uploads under `/api/deployments`.
Local `deploy` loops bounded reader/sync passes so dynamic data sources created
by mappings are subscribed, backfilled, and indexed before the command reports
success.

Deployment names are unique inside one Postgres-backed instance. The public API
serves the current deployment through Graph Node/Goldsky-style paths:
`/subgraphs/<deployment>/<version>/gn`,
`/subgraphs/<deployment>/<version>/graphql`, and the `latest` alias. Explicit
version paths are accepted only when they match registered deployment metadata.
Hosted instances can choose their deploy policy with `UGRAPH_DEPLOY_AUTH_MODE`:
`owner` allows only the deployment owner or an admin API key to update owned
deployments, while `open` allows any key with `deploy` scope to publish.

## Local Development

```bash
cargo test
cargo run -p ugraph -- doctor --manifest core/examples/growfi/subgraph.yaml
cargo run -p ugraph-node -- serve --help
docker build -f core/Dockerfile -t ugraph-core:local .
```

## Infra

`infra/` owns online deployment wiring. The lowest-cost target is currently
`infra/gcp/e2-micro`: one Google Compute Engine `e2-micro` VM, local Docker
Compose, local Postgres, and direct image upload without Cloud SQL, Cloud Run,
or Artifact Registry. The public edge is Caddy HTTPS on either a custom domain
such as `ugraph.growfi.dev` or the generated `<external-ip>.sslip.io` hostname;
the API and Postgres stay internal to the Docker network.
