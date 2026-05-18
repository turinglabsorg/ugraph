# ugraph Infra

This layer will package and run `core` online.

Initial target:

- Docker image for local and cloud execution.
- Cloud Run service for the API/query process.
- Managed Postgres for the canonical entity store.
- GraphQL endpoint plus GraphiQL UI.

Redis is intentionally out of scope for the first deployment path.

## Local Postgres

```bash
docker compose -f infra/docker-compose.yml up -d postgres
```

## CLI Image

```bash
docker build -f infra/Dockerfile -t ugraph-core .
docker run --rm ugraph-core --help
```

The same image shape can be promoted to Cloud Run once the query/API service is
implemented.

The builder image uses Rust 1.88+ because Wasmtime 38 requires it.
