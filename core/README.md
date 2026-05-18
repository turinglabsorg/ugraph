# ugraph

`ugraph` is a fast Rust runtime for standard Graph Protocol subgraphs.

The goal is full compatibility with the deployment shape used by The Graph,
Goldsky, and similar hosted providers: existing `subgraph.yaml`,
`schema.graphql`, ABI files, templates, event handlers, and compiled mapping
WASM should be loadable without rewriting the subgraph for a proprietary format.

## Design

- Standard `subgraph.yaml` as the source of truth.
- Rust CLI and runtime.
- Serverless-friendly sync loops.
- Pluggable storage backends.
- WASM mapping runtime using Graph host exports.
- GraphQL query compatibility for generated subgraph schemas.
- RPC resolution from user/env config first, then a fresh Chainlist registry fallback.

## Commands

```bash
cd core
cargo test
cargo run -p ugraph -- validate --manifest examples/growfi/subgraph.yaml
cargo run -p ugraph -- inspect --manifest examples/growfi/subgraph.yaml
cargo run -p ugraph -- rpc --chain-id 11155111
cargo run -p ugraph -- compat --build-dir examples/growfi/build
cargo run -p ugraph -- runtime-check --build-dir examples/growfi/build
cargo run -p ugraph -- handler-exports --manifest examples/growfi/subgraph.yaml
cargo run -p ugraph -- handler-signatures --manifest examples/growfi/subgraph.yaml
cargo run -p ugraph -- abi-events --manifest examples/growfi/subgraph.yaml
cargo run -p ugraph -- plan --manifest examples/growfi/subgraph.yaml
cargo run -p ugraph -- schema --manifest examples/growfi/subgraph.yaml
cargo run -p ugraph -- scan --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --from-block 10845295 --to-block 10846000
cargo run -p ugraph -- replay --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --from-block 10845480 --to-block 10845480
cargo run -p ugraph -- sync --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --from-block 10845295 --to-block 10846000
cargo run -p ugraph -- sync --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --watch --poll-interval-ms 1000
cargo run -p ugraph -- serve --state-file .ugraph/state.json --port 8030
cargo run -p ugraph -- compare --state-file .ugraph/state.json --endpoint <hosted-graphql-url> --query '<graphql>'
cargo run -p ugraph -- conformance --state-file .ugraph/state.json --endpoint <hosted-graphql-url> --cases-file examples/growfi/conformance.json
cargo run -p ugraph -- matrix --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --to-block 10846000 --endpoint <hosted-graphql-url> --cases-file examples/growfi/conformance.json
cargo run -p ugraph -- sync --manifest examples/growfi/subgraph.yaml --storage postgres --deployment growfi --postgres-url <postgres-url> --rpc-url <rpc>
cargo run -p ugraph -- chain-reader --manifest examples/growfi/subgraph.yaml --postgres-url <postgres-url> --deployment growfi --chain-id 11155111 --rpc-url <rpc> --watch
cargo run -p ugraph -- sync --manifest examples/growfi/subgraph.yaml --storage postgres --deployment growfi --postgres-url <postgres-url> --log-source postgres-feed --chain-id 11155111 --rpc-url <rpc>
cargo run -p ugraph -- users --postgres-url <postgres-url> create --email ops@example.com --role admin
cargo run -p ugraph -- users --postgres-url <postgres-url> key create --email ops@example.com --name cli --scope deploy --scope query
cargo run -p ugraph -- users --postgres-url <postgres-url> signup status
cargo run -p ugraph -- users --postgres-url <postgres-url> signup disable
cargo run -p ugraph -- deploy --provider local --manifest examples/growfi/subgraph.yaml --storage postgres --postgres-url <postgres-url> --deployment growfi --version v1 --visibility public --api-key <ugraph-api-key> --chain-id 11155111 --rpc-url <rpc>
cargo run -p ugraph -- deployments --postgres-url <postgres-url> list
cargo run -p ugraph -- deployments --postgres-url <postgres-url> register --deployment growfi --version v1 --visibility public
cargo run -p ugraph -- deployments --postgres-url <postgres-url> set-visibility --deployment growfi --visibility private
cargo run -p ugraph -- serve --storage postgres --deployment growfi --postgres-url <postgres-url> --port 8030
cargo run -p ugraph -- doctor --manifest examples/growfi/subgraph.yaml
docker build -t ugraph-core:local .
docker compose up --build
```

## Environment

`.env.example` documents the supported local variables:

```bash
UGRAPH_CHAIN_ID=11155111
UGRAPH_RPC_URL=https://ethereum-sepolia-rpc.publicnode.com
UGRAPH_STATE_FILE=.ugraph/state.json
UGRAPH_STORAGE=json
UGRAPH_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres
UGRAPH_DEPLOYMENT=default
UGRAPH_LOG_SOURCE=rpc
UGRAPH_POLL_INTERVAL_MS=1000
UGRAPH_RETRY_MAX_MS=60000
UGRAPH_REORG_POLICY=rollback
UGRAPH_REORG_CHECK_DEPTH=64
UGRAPH_HISTORY_LIMIT=1024
UGRAPH_MAX_BLOCK_RANGE=2000
UGRAPH_RPC_RETRIES=3
UGRAPH_RPC_TIMEOUT_SECS=15
UGRAPH_DEPLOY_MAX_PASSES=8
UGRAPH_SYNC_LIMIT=1000
UGRAPH_API_KEY=
UGRAPH_IPFS_GATEWAY=https://ipfs.io/ipfs/
UGRAPH_IPFS_TIMEOUT_SECS=60
UGRAPH_MAX_IPFS_FILE_BYTES=26214400
UGRAPH_HOST=127.0.0.1
UGRAPH_PORT=8030
```

## Container Runtime

The image uses the same binary for the API and indexer:

- `UGRAPH_MODE=serve` exposes `/graphql`, `/`, `/status`, `/healthz`, and
  `/metrics`. It also accepts hosted-provider compatible query paths at
  `/subgraphs/<deployment>/<version>/gn` and
  `/subgraphs/<deployment>/<version>/graphql`.
- `UGRAPH_MODE=indexer` runs `sync --watch`.
- `UGRAPH_MODE=chain-reader` reads one `chain_id` from RPC and writes raw logs
  into Postgres for every registered subscription on that chain. If no explicit
  RPC URL is configured, it tries resolved Chainlist URLs in order. Before
  appending new logs, it checks feed cursor hashes against the selected RPC and
  rolls back raw blocks/logs plus affected cursors from the first mismatched
  block.
- `UGRAPH_STORAGE=postgres` should be used for shared API/indexer deployments.
- `UGRAPH_LOG_SOURCE=rpc|postgres-feed` controls whether `sync` reads logs
  directly from RPC or from the shared Postgres raw feed. The default remains
  RPC for direct compatibility; local deployment uses `postgres-feed`.
- `UGRAPH_REORG_POLICY=fail|rollback|reset` controls what happens when the
  stored checkpoint hash no longer matches the RPC. `rollback` rewinds to the
  newest retained matching checkpoint.
- `UGRAPH_REORG_CHECK_DEPTH` bounds how many retained checkpoints are probed
  during rollback.
- `UGRAPH_HISTORY_LIMIT` controls retained current-state historical snapshots;
  `0` keeps all retained snapshots.
- `UGRAPH_MAX_BLOCK_RANGE` chunks `eth_getLogs` ranges for provider safety.
- `UGRAPH_RPC_RETRIES` retries transient RPC and HTTP failures.
- `UGRAPH_RPC_TIMEOUT_SECS` bounds individual RPC and registry HTTP requests.
- `UGRAPH_IPFS_GATEWAY` configures the gateway used by `ipfs.cat`,
  `ipfs.getBlock`, and `ipfs.map`; it can be a prefix or contain a `{path}`
  placeholder.
- `UGRAPH_IPFS_TIMEOUT_SECS` and `UGRAPH_MAX_IPFS_FILE_BYTES` bound IPFS
  gateway calls.
- `UGRAPH_API_KEY` authenticates CLI operations that record deployment
  ownership and can also be sent to private query endpoints with
  `Authorization: Bearer <key>` or `x-api-key`.

`docker-compose.yml` starts Postgres, a shared `chain-reader`, a feed-backed
indexer worker, and the API locally. The API reloads the selected store on each
GraphQL request, so writes committed by the indexer are visible without
restarting the server.

Production should evolve toward a shared chain feed. Instead of every subgraph
deployment calling `eth_getLogs` for the same chain, one `chain-reader` per
`chain_id` should write raw block/log data into Postgres. Deployment-specific
sync jobs then consume matching raw logs from that feed and write entities
under their own `UGRAPH_DEPLOYMENT` ids. A deployment can subscribe to multiple
`chain_id` values, with chain-scoped cursors and raw feed data. This keeps
multi-subgraph deployments cheap without turning the runtime into one heavy
multi-subgraph process.

`ugraph deploy --provider local` currently registers static subscriptions and,
when `--log-source postgres-feed` is selected, runs bounded `chain-reader` and
`sync` passes until dynamically created data sources have been backfilled or
`UGRAPH_DEPLOY_MAX_PASSES` is reached. It fails if the checkpoint remains
incomplete, and leaves the API available through the normal `serve` command.
When Postgres storage is used, `deploy` also records deployment metadata:
version label, visibility, owner, and the API key that created or updated the
deployment.

Deployment ids are unique in a Postgres-backed instance. The versioned query
paths accept only the selected deployment name. `latest` aliases the current
deployment, while explicit version labels must match registered metadata. Use
`ugraph deployments register` to update version or visibility metadata without
running a sync.

## Users and API keys

Postgres is the identity source for the core control plane. Users are keyed by
normalized email and API keys are stored as hashes only; the secret is printed
once when it is created.

```bash
ugraph users --postgres-url <postgres-url> create \
  --email ops@example.com \
  --display-name "Ops" \
  --role admin

ugraph users --postgres-url <postgres-url> key create \
  --email ops@example.com \
  --name cli \
  --scope deploy \
  --scope query

UGRAPH_API_KEY=<ugraph-api-key> \
ugraph deploy --provider local \
  --manifest examples/growfi/subgraph.yaml \
  --storage postgres \
  --postgres-url <postgres-url> \
  --deployment growfi \
  --version v1 \
  --visibility public
```

`users signup enable|disable|status` controls whether public user creation is
allowed by future HTTP control-plane endpoints. The current default is
disabled. Query serving uses deployment metadata: deployments marked `public`
are open, deployments marked `private` require an API key with `query` scope.
Deployments without metadata remain public so existing local and cloud
instances are not locked out during upgrades.

GraphiQL is available at `/graphql` and can also be opened from a versioned
endpoint such as `/subgraphs/growfi/v1/gn`; in that case it posts queries back
to the same endpoint.

## Current Scope

The first milestone is compatibility plumbing:

- Parse and validate standard subgraph manifests.
- Resolve schema, mappings, and ABI references.
- Preserve `dataSources` and `templates` semantics.
- Execute decoded Ethereum logs against compiled graph-ts WASM handlers.
- Validate `store.set` entity payloads against `schema.graphql`.
- Persist current-state snapshots with checkpoint metadata through `sync`.
- Persist current-state deployments to Postgres through `--storage postgres`.
- Persist shared raw chain feed tables for subscriptions, raw blocks, and raw
  logs keyed by `chain_id`.
- Persist retained historical entity versions as compact delta rows with
  tombstones for removals.
- Persist users, hashed API keys, public-signup configuration, and deployment
  ownership metadata for CLI-driven deploys.
- Run continuous current-state indexing with `sync --watch`.
- Prevent two Postgres-backed indexers from syncing the same deployment
  concurrently with a session-level advisory lock.
- Serve GraphQL and GraphiQL from the current-state snapshot through `serve`.
  The query endpoint supports POST/GET `/graphql`, Graph Node/Goldsky-style
  versioned paths, CORS preflight, `operationName`, variables, `where`, nested
  relations, `@derivedFrom`, ordering, pagination, named fragments, inline
  fragments, `@include`, `@skip`, `_meta`, and generated schema introspection.
- Run batch hosted-provider diffs through `conformance` using JSON case files.
- Run `matrix` as the repeatable compatibility gate. It emits one report that
  combines structural `doctor`, optional bounded sync, and optional hosted
  GraphQL conformance.
- Run local deploys through `ugraph deploy --provider local`.
- Run compatibility checks against large public subgraphs. The current Uniswap
  v3 mainnet stress fixture builds from the official `Uniswap/v3-subgraph` and
  passes `doctor`, `compat`, handler export, and handler signature checks with
  no missing host exports.
- Run compatibility checks against additional public subgraphs. The official
  Aave v3 mainnet subgraph builds and replays successfully for an initial
  mainnet window; `doctor` flags one upstream manifest/export mismatch.

GrowFi lives under `examples/growfi/` as a real compatibility fixture, not as
hardcoded runtime logic.

See `docs/graph-node-compatibility.md` for the concrete Graph Node surface that
must be replicated before claiming Goldsky equivalence.

## Target Runtime

The runtime executes the same compiled mapping WASM that hosted subgraph
providers run. `replay` is the first end-to-end path: scan JSON-RPC logs,
decode ABI params, allocate a graph-ts `EthereumEvent`, and call the exported
handler. It also decodes graph-ts `Entity` values passed to `store.set` and
keeps an in-memory entity store that can be read back through `store.get`.
Within one `replay` run, that store is shared across executed logs.
Dynamic template spawns through `dataSource.create` are captured as template
name plus string params. `replay` turns those calls into manifest-backed
dynamic sources, scans the created addresses, and queues their logs.
`dataSource.createWithContext` persists context entities with the dynamic source
snapshot in JSON/Postgres and restores them through `dataSource.context` for
template handlers. BigInt host
arithmetic is implemented for the operations imported by the current real
fixtures, including `bigInt.dividedByDecimal`. BigDecimal arithmetic is
implemented for the operations imported by larger graph-ts mappings, including
Uniswap v3 tick math. Values are clamped to 34 significant digits to match The
Graph's documented decimal128-style BigDecimal precision and avoid unbounded
coefficient growth. The runtime also decodes dynamic ABI `string` and `bytes`
return values for `ethereum.call`, so generated ERC20 `name()` and `symbol()`
calls return proper graph-ts strings. `bytesToString` trims trailing null bytes
for right-padded fixed bytes, matching common graph-ts `bytes32.toString()`
usage in subgraphs such as Aave. The runtime implements graph-ts JSON host
exports used by IPFS/file mappings (`json.fromBytes`, `json.try_fromBytes`,
numeric conversion helpers) and IPFS host exports (`ipfs.cat`, `ipfs.getBlock`,
and `ipfs.map`). IPFS fetches use `UGRAPH_IPFS_GATEWAY`; `ipfs.map` supports
Graph Node's JSON-line behavior by parsing each non-empty line as one
graph-ts-compatible `JSONValue` and invoking the mapping callback for each
line.
Static ABI contract reads through graph-ts `ethereum.call` are executed as
JSON-RPC `eth_call` at the handler block. Historical replay needs an
archive-capable RPC; falling back to `latest` would break determinism.
`sync` writes the current entity store, schema, dynamic source instances,
checkpoint, historical current-state snapshots, and partial-run processed-log
cursor to either a JSON snapshot or a Postgres deployment. Postgres storage uses
normalized current-state tables for deployments, entities, dynamic sources, and
processed-log cursors. Retained historical checkpoints are also persisted in
separate Postgres history tables with compact entity-version deltas per
retained block and tombstones for removals, while keeping the same
`StoreSnapshot` load path used by the query server.
`sync --watch` repeats that same deterministic pass on a polling interval for
live indexing. Transient sync failures are logged as JSON and retried with
capped exponential backoff using `UGRAPH_RETRY_MAX_MS`.
Postgres-backed `sync` holds a session-level advisory lock for the selected
deployment during the process lifetime. A second indexer for the same
deployment exits instead of racing writes.
Each replay/sync run compiles a distinct mapping WASM module once and reuses
the compiled module for fresh per-log instances. Handler writes run against
candidate store/cache state and commit only after schema validation passes, so
invalid `store.set` payloads do not mutate the run store or spawn dynamic
sources. `--limit` is treated as a soft cap at block boundaries: once a block
starts, all remaining logs in that block are processed before the run stops,
and retained historical snapshots are only written after a complete block.
`eth_getLogs` ranges are chunked with `UGRAPH_MAX_BLOCK_RANGE`, retried with
`UGRAPH_RPC_RETRIES`, and split further on provider range-limit failures.
Before resuming from a stored checkpoint, `sync` compares the checkpoint block
hash against the selected RPC. The default `rollback` policy probes retained
historical checkpoints and rewinds to the newest checkpoint whose hash still
matches the RPC. `fail` stops on mismatch, while `reset` discards the
current-state snapshot and rebuilds from the manifest start block. `serve`
loads the selected store on every GraphQL request and exposes a
GraphQL envelope plus the classic GraphiQL UI at `/graphql`, including filters,
variables, nested relations, derived relations, historical `block: { number }`
and `block: { hash }` selection, fragments, directives, `_meta.block.hash`, and
introspection over the current-state entity model. `/status` is a small HTML
operational page, while `/metrics` exposes Prometheus gauges for store
availability, entity count, history count, history block range, checkpoint
block, completion state, and validation errors.

## Heavy Subgraph Fixture

The official Uniswap v3 subgraph is the first large public stress fixture:

- `ugraph doctor` and `compat` pass for 1 static data source, 1 template, 2
  WASM modules, 6 handlers, and 20 required host imports.
- A 10,000-block `PoolCreated` scan from mainnet block `12369621` found 406 logs
  in 11.65 seconds against a public RPC.
- A first-log sync produced the expected UNI/WETH pool, correct ERC20 token
  metadata, 5 entities, 1 dynamic source, and 0 validation errors in 7.98
  seconds.
- Before BigDecimal precision limiting, a 25-log stress sync across the first
  1,000 blocks produced 85 entities, 7 dynamic sources, and 0 validation errors
  in 131.94 seconds.
- After BigDecimal precision limiting, pow10 caching, shared HTTP client reuse,
  and per-run `eth_call` result caching, the same 25-log stress sync completed
  in 14.96 seconds with the same counts and 0 validation errors.
- A complete first-1,000-block sync processed all 85 available logs in 27.27
  seconds, producing 254 entities, 17 dynamic sources, and 0 validation errors.
- With per-run WASM module caching, a release-mode rerun of the same 25-log
  first-1,000-block stress slice completed in 9.27 seconds wall clock, with 85
  entities, 7 dynamic sources, and 0 validation errors.
- `ugraph matrix` reports this fixture as structurally and synchronously OK for
  the tested 25-log first-1,000-block slice.

The result is functionally correct for the tested slice and no longer dominated
by unbounded BigDecimal growth. The next performance work is release-mode
benchmarking, module precompilation/reuse, and broader backfill tests.

## Additional Public Fixtures

The official Aave v3 mainnet subgraph builds from `aave/protocol-subgraphs`
with modern `graph-cli` using `VERSION=v3 BLOCKCHAIN=v3 NETWORK=mainnet`:

- Shape: 3 static data sources, 8 templates, 8 compiled WASM modules, 73 event
  handlers, and 21 required host imports.
- `compat` reports no missing host exports and `abi-events` passes after
  accepting artifact JSON files with a top-level `abi` field.
- `doctor` reports one upstream mismatch: the manifest declares
  `PoolConfigurator.handleReserveActive`, but the compiled WASM exports
  `handleReserveActivated`.
- Replay over mainnet blocks `16291006..16292006` executed 12 logs, created
  `PoolAddressesProvider`, `Pool`, and `PoolConfigurator` dynamic sources, and
  produced 0 validation errors.
- Sync over the same range completed in 6.92 seconds wall clock with 5
  entities, 3 dynamic sources, complete checkpoint `16292006`, and 0 validation
  errors. The same slice completed in 4.71 seconds wall clock in release mode
  after per-run WASM module caching.
- Local `/healthz` and `/graphql` served the Aave snapshot successfully.
- `ugraph matrix` reports `structural=false` because of the upstream
  manifest/export mismatch, while `sync.ok=true` for the tested slice.

The official Compound v2 subgraph is tracked as a legacy tooling fixture:
`apiVersion: 0.0.3` is rejected by modern `graph-cli`, and older CLI versions
currently fail from historical install/build dependencies in the current npm
environment. That is not yet a runtime compatibility result.

The BAYC IPFS fixture `syamantak01/BoredApeYachtClub-API` uses `ipfs.cat` plus
`json.fromBytes` to load NFT metadata from IPFS. The clone at
`/private/tmp/ugraph-bayc-ipfs` includes built WASM. `ugraph doctor` passes with
8 required host imports and no missing exports. Matrix over Ethereum mainnet
block `12292922` with `UGRAPH_IPFS_GATEWAY=https://dweb.link/ipfs/` executed 30
Transfer logs, produced 31 entities, and produced 0 validation errors.

## RPC Resolution

`ugraph` never requires a baked-in vendor RPC. Resolution order:

1. Explicit CLI `--rpc-url`
2. Env `UGRAPH_RPC_URL`, `RPC_URL`, or `ETH_RPC_URL`
3. Fresh Chainlist-compatible registry fetch from `https://chainid.network/chains.json`

The fallback filters out WSS endpoints and placeholder URLs such as
`${INFURA_API_KEY}` because the first sync path uses HTTP JSON-RPC.
