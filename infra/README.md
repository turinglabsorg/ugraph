# ugraph Infra

This layer will package and run `core` online.

Initial target:

- Docker image for local and cloud execution.
- Shared Postgres for the raw chain feed and deployment entity stores.
- One chain reader per `chain_id`.
- Deployment-specific sync workers/jobs that consume the shared feed.
- API/query process per deployment.
- GraphQL endpoint plus GraphiQL UI.

Redis is intentionally out of scope for the first deployment path.

## Deployment Model

The infra layer should hide provider wiring behind one CLI command. Operators
should not manually assemble Cloud Run services, jobs, schedulers, secrets, and
database tables for every subgraph.

The target shape is:

- `chain-reader:<chain-id>` reads RPC once and writes raw blocks/logs to
  Postgres.
- `sync:<deployment>` consumes raw logs from Postgres and writes entities under
  that deployment id.
- `api:<deployment>` serves GraphQL/GraphiQL from the deployment store.

This lets many subgraphs and versions share one chain reader and one Postgres
instance while keeping entity data isolated by deployment id. A deployment can
subscribe to more than one `chain-reader:<chain-id>` when the subgraph spans
multiple chains.

## Local Postgres

```bash
docker compose -f infra/docker-compose.yml up -d postgres
```

The implemented local core compose lives in `core/docker-compose.yml` and
starts Postgres, `chain-reader`, `indexer`, and `api`. The indexer uses
`UGRAPH_LOG_SOURCE=postgres-feed` by default there, while direct RPC sync
remains available with `UGRAPH_LOG_SOURCE=rpc`.

## CLI Image

```bash
docker build -f infra/Dockerfile -t ugraph-core .
docker run --rm ugraph-core --help
```

The same image shape can be promoted to managed container hosts once provider
automation is implemented.

The builder image uses Rust 1.88+ because Wasmtime 38 requires it.
