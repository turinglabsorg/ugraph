# Architecture

`ugraph` is a Rust runtime for standard Graph Protocol subgraphs. The project
is intentionally agnostic: GrowFi is only the first fixture under
`core/examples/growfi/`.

## Components

- `core/crates/ugraph-core`: standard subgraph manifest, GraphQL schema, ABI, and runtime types.
- `cli`: CLI entrypoint for validate, inspect, sync, serve, and deploy.
- `ugraph-runtime`: executes graph-ts mapping WASM and implements Graph host exports.
- Stores: JSON persistence for local sync passes and Postgres persistence for
  deployment-backed sync/serve loops. Both store schema, entities, dynamic
  source instances, checkpoint, retained historical snapshots, and partial-run
  processed-log cursors. Postgres stores retained historical checkpoints and
  compact entity-version deltas in separate tables. The append-only
  `ugraph_entity_changes` timeline is separate from retained history and records
  when entities were created, updated, or removed.
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

## Shared Chain Feed

Production indexing should not make every subgraph deployment read the same
chain independently. The low-cost production model is a shared multi-chain
feed:

1. One `chain-reader` per `chain_id`/RPC reads blocks and logs in canonical
   order.
2. The reader writes raw block metadata and raw logs into append-only Postgres
   tables keyed by `chain_id`, `block_number`, `block_hash`,
   `transaction_index`, and `log_index`.
3. Subgraph sync jobs read matching raw logs from that local feed using the
   manifest's addresses, topics, and dynamic data source subscriptions.
4. Mapping WASM execution and entity writes stay isolated by `deployment`.
5. The GraphQL API reads only the entity store for its deployment.

This means one RPC reader can serve many subgraphs and many versions on the
same chain, while separate readers can serve other chains in the same Postgres
instance. It also creates a natural cache for retries, reorg checks, and
backfills: replaying a subgraph should not require re-fetching logs that the
chain reader has already persisted.

Multi-chain subgraphs are represented as multiple subscriptions under one
deployment. Each subscription points to a `chain_id` and a manifest data source
or dynamic source. Entity writes remain scoped by deployment, while raw chain
data remains scoped by chain.

The first implementation can keep the feed inside Postgres to avoid operating
Kafka, Pub/Sub, Redis, or another queue. Later, the same boundary can be backed
by a streaming system if throughput requires it. The contract between reader
and sync jobs is raw chain data, not decoded entity changes.

Implemented feed tables:

- `ugraph_feed_subscriptions` stores deployment/source/address/topic
  subscriptions and a cursor per `chain_id`.
- `ugraph_raw_blocks` stores observed block hashes per `chain_id`.
- `ugraph_raw_logs` stores raw logs keyed by `chain_id`, block, transaction
  index, and log index.

`ugraph chain-reader` reads all active subscriptions for one `chain_id` and
writes raw logs into those tables. When no explicit RPC URL is configured, it
tries resolved Chainlist URLs in order. `ugraph sync --log-source
postgres-feed` loads matching logs from Postgres instead of calling
`eth_getLogs`. The direct RPC path remains available through `--log-source rpc`.
Before appending new logs, `chain-reader` validates stored subscription cursor
hashes against the selected RPC. On mismatch it treats the raw feed as reorged:
raw blocks/logs are pruned from the first mismatched block and every affected
subscription cursor for that chain is rewound.

Target CLI flow:

```bash
ugraph deploy \
  --provider local \
  --deployment growfi-v1 \
  --chain-id 11155111 \
  --manifest subgraph.yaml \
  --postgres-url <url> \
  --rpc-url <rpc>
```

The deploy command should:

- Create or reuse the shared Postgres database.
- Ensure a `chain-reader` exists for every requested `chain_id`.
- Register the subgraph deployment and its static subscriptions.
- Loop bounded chain-reader/sync passes until dynamic data source subscriptions
  created by mappings are backfilled and the checkpoint is complete.
- Build/publish the container image when needed.
- Start or schedule sync workers for that deployment.
- Expose the GraphQL/GraphiQL API for that deployment.

The operator should not have to manually wire Cloud Run services, jobs,
schedulers, or database tables.

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

For the shared-feed model, the same image supports
`UGRAPH_MODE=chain-reader`. That process owns RPC polling for one `chain_id`
and writes raw blocks/logs to Postgres. Run one reader per `chain_id`.
`UGRAPH_MODE=indexer` then consumes local raw logs instead of calling RPC
directly when `UGRAPH_LOG_SOURCE=postgres-feed` is configured.

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
Individual RPC, Chainlist registry, and mapping `ethereum.call` requests are
bounded by `UGRAPH_RPC_TIMEOUT_SECS`. Transient HTTP/RPC failures are retried
with `UGRAPH_RPC_RETRIES`; provider range-limit failures are split recursively
into smaller ranges. Initial static source scanning tries resolved RPC URLs in
order, so a bad public endpoint can fall through to the next Chainlist URL or
explicit configured URL.

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
