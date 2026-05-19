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
docker build -f infra/Dockerfile -t ugraph-cli .
docker run --rm ugraph-cli --help
```

This image is for user/operator commands. The production core runtime image is
built from `core/Dockerfile` and runs `ugraph-node` only.

The builder image uses Rust 1.88+ because Wasmtime 38 requires it.

## Lowest-Cost GCP Target

`gcp/e2-micro/` deploys the core image to one Google Compute Engine `e2-micro`
VM in an Always Free eligible US region. It avoids Cloud SQL, Cloud Run, and
Artifact Registry by uploading the locally built `linux/amd64` Docker image
archive directly to the VM and running local Docker Compose.

The default e2-micro profile is compact: Postgres, direct-RPC indexer, and API.
The shared feed profile remains available with `COMPOSE_PROFILES=feed` plus
`UGRAPH_LOG_SOURCE=postgres-feed`, but it is intentionally not the default on a
1 GiB VM.

The e2-micro target uses Caddy for HTTPS on `ugraph.growfi.dev` when
`DO_DNS_ZONE=growfi.dev` is provided, or an `sslip.io` hostname when no custom
domain is configured. It opens only ports `80` and `443`, keeps Postgres
internal, stores the generated Postgres password in `/opt/ugraph/.env` with
`0600` permissions, and enables a 2 GiB swapfile plus unattended security
updates during VM bootstrap.
