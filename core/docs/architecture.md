# Architecture

`ugraph` is a Rust runtime for standard Graph Protocol subgraphs. The project
is intentionally agnostic: GrowFi is only the first fixture under
`examples/growfi/`.

## Components

- `ugraph-core`: standard subgraph manifest, GraphQL schema, ABI, and runtime types.
- `ugraph-cli`: CLI entrypoint for validate, inspect, sync, serve, and deploy.
- `ugraph-runtime`: executes graph-ts mapping WASM and implements Graph host exports.
- Stores: JSON persistence for local sync passes and Postgres persistence for
  deployment-backed sync/serve loops. Both store schema, entities, dynamic
  source instances, checkpoint, retained historical snapshots, and partial-run
  processed-log cursors. Postgres stores retained historical checkpoints and
  compact entity-version deltas in separate tables.
- `server`: GraphQL endpoint compatible with hosted subgraph query envelopes
  for current-state entity reads.
- `rpc`: user-provided RPC first, then fresh Chainlist-compatible registry fallback.
- `ugraph-runtime` must implement the Graph host export surface documented in
  `graph-node-compatibility.md`; executing custom Rust reducers is not enough
  for Goldsky compatibility.

## Sync Model

Each sync pass scans from each data source cursor to a target block.

1. Parse `subgraph.yaml`.
2. Register static data sources.
3. Fetch matching logs by ABI event signature.
4. Execute the mapped WASM handler with Graph-compatible host functions.
5. Validate entity payloads against `schema.graphql`.
6. Persist current-state entity mutations, checkpoint metadata, and partial-run
   cursors when the process stops before the target block is complete.
7. Instantiate and persist templates when mappings call `Template.create`.
8. In `--watch` mode, repeat the same sync pass after `UGRAPH_POLL_INTERVAL_MS`
   so the deployment keeps following chain head. Transient failures do not
   crash the worker; they are logged as JSON and retried with capped
   exponential backoff.

This mirrors hosted subgraph semantics while keeping the operational footprint
small enough for serverless or cron-style execution.

## Query Model

The endpoint accepts normal GraphQL request envelopes:

```json
{ "query": "...", "variables": { "id": "..." } }
```

Current support covers POST and GET `/graphql`, CORS preflight, GraphiQL,
`operationName`, variables, `_meta`, entity lookup by ID, plural entity lists,
`where`, nested direct relations, `@derivedFrom`, `first`, `skip`, `orderBy`,
`orderDirection`, named fragments, inline fragments, `@include`, `@skip`,
generated schema introspection, and Graph scalar JSON output for the stored
current state.

## Storage Model

Postgres is the canonical production store target. The current core implements
two backends:

- JSON snapshots for local development and file-based serverless smoke tests.
- Postgres current-state storage for deployments, entities, dynamic source
  instances, and partial-run processed-log cursors.
- Postgres historical storage for retained checkpoints and entity versions per
  retained block. Entity versions are stored as compact deltas with tombstones
  for removals and materialized back into snapshots at load time.

Postgres writes are transactional and can back `sync`, `serve`, `compare`, and
`conformance` through `--storage postgres --deployment <id> --postgres-url
<url>`. Sync retains historical current-state snapshots. Root entity queries
and `_meta` can select `block: { number }` using the latest retained snapshot at
or before that block, or `block: { hash }` using an exact retained checkpoint
hash. Current-state sync detects checkpoint hash mismatches before resuming.
The default `rollback` policy probes retained checkpoints and rewinds to the
newest matching checkpoint. `fail` stops on mismatch, while `reset` rebuilds
from the manifest start block. A Postgres-backed indexer also holds a
session-level advisory lock, so two workers cannot write the same deployment at
the same time. Full Graph Node parity still needs true block-range compression
for arbitrary historical density.

## Container Model

The core ships a single Docker image. `UGRAPH_MODE=serve` runs the API, and
`UGRAPH_MODE=indexer` runs the live `sync --watch` worker. The same image can be
used on a local Docker host, DigitalOcean App Platform, or any container
runtime. `docker-compose.yml` is the local production-shaped smoke with
Postgres, indexer, and API.

The API reloads the selected store for each GraphQL request. This keeps the
server simple and makes indexer writes visible immediately after the Postgres
transaction commits. Later we can add an in-process cache with block-aware
invalidation if query volume requires it.

The API also exposes `/metrics` in Prometheus text format. These gauges are
intentionally deployment-level and low-cardinality: store availability, entity
count, dynamic source count, history count, history block range, checkpoint
block, completion state, and validation error count. `/status` is a tiny HTML
status page for operators and smoke checks.

## RPC Robustness

Log scanning chunks `eth_getLogs` requests with `UGRAPH_MAX_BLOCK_RANGE`.
Transient HTTP/RPC failures are retried with `UGRAPH_RPC_RETRIES`; provider
range-limit failures are split recursively into smaller ranges. Initial static
source scanning tries resolved RPC URLs in order, so a bad public endpoint can
fall through to the next Chainlist URL or explicit configured URL.

## Live Smoke

The live GrowFi Sepolia smoke buys campaign tokens on the real v4 deployment,
then checks the resulting `Purchase` through `ugraph` and Goldsky:

- Campaign: Olive Sicily `0x59e24007A065eB99c8C8C0325287E359eA4F41de`
- Payment token: MockUSDC `0x341BE87780d6CE9F7785900d3245Cb61fb3B1aE1`
- Transaction: `0x0ce83b9006ae4a7ce985505f6eee0e52b54d9ed07a0f0c4d76bee95bb1df3c25`
- Block: `10866837`
- Sync result from the previous snapshot: 4 executed logs, 0 validation errors,
  8 seconds wall clock including a rebuild; the local GraphQL response matched
  Goldsky exactly for the new purchase.

SQLite can support lightweight local tests. Redis is out of scope.

## Infra Boundary

The `infra/` folder owns Docker, Cloud Run, managed Postgres, secrets, and
deployment wiring. The core must remain runnable locally without cloud services.

## Equivalence Gate

Compatibility is measured by response equality against a hosted Graph Node
provider for a fixed block. Manifest parsing alone is not sufficient. The
`conformance` command reads a JSON array of GraphQL cases and compares UGraph
responses with the hosted endpoint after normalization.
