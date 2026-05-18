# Core Notes

## Role

This workspace is the `ugraph` core: the Rust compatibility layer for Graph
Node/Goldsky subgraphs.

## Commands

Run from this directory:

```bash
cargo test
cargo run -p ugraph -- doctor --manifest examples/growfi/subgraph.yaml
cargo run -p ugraph -- replay --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --from-block <n> --to-block <n>
cargo run -p ugraph -- sync --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --state-file .ugraph/state.json --from-block <n> --to-block <n>
cargo run -p ugraph -- sync --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --state-file .ugraph/state.json --watch --poll-interval-ms 1000
cargo run -p ugraph -- serve --state-file .ugraph/state.json --port 8030
cargo run -p ugraph -- compare --state-file .ugraph/state.json --endpoint <hosted-graphql-url> --query '<graphql>'
cargo run -p ugraph -- conformance --state-file .ugraph/state.json --endpoint <hosted-graphql-url> --cases-file examples/growfi/conformance.json
cargo run -p ugraph -- matrix --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --to-block <n> --endpoint <hosted-graphql-url> --cases-file examples/growfi/conformance.json
cargo run -p ugraph -- sync --manifest examples/growfi/subgraph.yaml --storage postgres --deployment <id> --postgres-url <url> --rpc-url <rpc> --from-block <n> --to-block <n>
cargo run -p ugraph -- chain-reader --manifest examples/growfi/subgraph.yaml --postgres-url <url> --deployment <id> --chain-id 11155111 --rpc-url <rpc> --watch
cargo run -p ugraph -- sync --manifest examples/growfi/subgraph.yaml --storage postgres --deployment <id> --postgres-url <url> --log-source postgres-feed --chain-id 11155111 --rpc-url <rpc> --from-block <n> --to-block <n>
cargo run -p ugraph -- deploy --provider local --manifest examples/growfi/subgraph.yaml --storage postgres --postgres-url <url> --deployment <id> --chain-id 11155111 --rpc-url <rpc>
cargo run -p ugraph -- serve --storage postgres --deployment <id> --postgres-url <url> --port 8030
docker build -t ugraph-core:local .
docker compose up --build
```

## Scope

- Parse standard subgraph manifests.
- Validate ABI event signatures and topic0 values.
- Inspect and instantiate compiled mapping WASM.
- Resolve compiled `build/subgraph.yaml` `mapping.file` entries when
  `graph-cli` deduplicates template WASM modules instead of emitting one WASM
  file per template name.
- Check handler exports and Graph Node handler ABI signatures.
- Resolve RPC endpoints and scan static Ethereum data sources.
- Execute decoded Ethereum logs against compiled graph-ts WASM handlers.
- Compile each distinct mapping WASM module once per replay/sync run through
  `RuntimeModuleCache`; handlers still get fresh instances/stores per log.
- Decode graph-ts `Entity` values from `store.set` into an in-memory store and
  rehydrate them through `store.get`.
- Keep one entity store across replayed logs so mappings can observe prior
  writes during the same replay run.
- Capture `dataSource.create` calls as template name plus string params.
- Capture `dataSource.createWithContext` context entities, persist them with
  dynamic source snapshots in JSON/Postgres, and expose them back to mappings
  through `dataSource.context`.
- Instantiate dynamic sources from the manifest template and queue their logs in
  `replay`.
- Support BigInt host arithmetic for `plus`, `minus`, `times`, `dividedBy`, and
  `dividedByDecimal`, and `pow`.
- Support BigDecimal host arithmetic for `fromString`, `plus`, `minus`,
  `times`, `dividedBy`, `equals`, and `toString`. BigDecimal values are clamped
  to decimal128-style 34 significant digits, matching The Graph's documented
  BigDecimal precision and preventing Uniswap v3 tick math from building
  unbounded integer coefficients. Keep zero-operation and large-exponent-gap
  shortcuts so simple comparisons/additions do not rescale huge powers
  unnecessarily.
- Decode dynamic ABI outputs for `ethereum.call` when functions return
  `string` or `bytes`; generated ERC20 calls such as `name()` and `symbol()`
  depend on this.
- Support `typeConversion.stringToH160` for graph-ts address conversions.
- Support graph-ts `bytesToString` conversion for right-padded fixed bytes by
  trimming trailing null bytes; Aave v3 uses `bytes32.toString()` to compare
  component IDs such as `POOL` and `POOL_CONFIGURATOR`.
- Support graph-ts JSON/IPFS host exports needed by file-backed mappings:
  `json.fromBytes`, `json.try_fromBytes`, numeric JSON conversions,
  `ipfs.cat`, `ipfs.getBlock`, and `ipfs.map`. IPFS uses
  `UGRAPH_IPFS_GATEWAY` with optional `{path}` placeholder, plus
  `UGRAPH_IPFS_TIMEOUT_SECS` and `UGRAPH_MAX_IPFS_FILE_BYTES`. `ipfs.map`
  follows Graph Node's JSON-line behavior: each non-empty line is parsed as one
  `JSONValue` and passed to the exported WASM callback.
- ABI checks accept both raw ABI arrays and compiler artifact JSON files with a
  top-level `abi` field.
- Support generic `ethereum.call` for static ABI calls via JSON-RPC `eth_call`
  at the handler block. Historical replay needs an archive RPC; never fall back
  to `latest`.
- Parse `schema.graphql` and validate runtime `store.set` payloads for entity
  types, required fields, scalar types, id consistency, unknown fields, and
  `@derivedFrom` writes. Multiline directives such as a field followed by
  `@derivedFrom(...)` on the next line are parsed as derived fields.
- `sync` persists current-state snapshots with schema, entity data, checkpoint,
  dynamic source instances, retained historical checkpoints, and partial-run
  processed-log cursors. A later sync resumes from the checkpoint and scans
  persisted dynamic sources without dropping unprocessed logs. `sync --watch`
  repeats this pass on a polling interval for live indexing. Watch mode logs
  transient failures as JSON and retries with capped exponential backoff.
  Reorg rollback is implemented through retained historical checkpoints.
  Handler execution uses candidate store/cache state and commits only after
  schema validation passes, so invalid `store.set` payloads do not mutate the
  run store or spawn dynamic sources. `sync`/`replay` treat `--limit` as a soft
  cap at block boundaries: once a block has started, all remaining logs in that
  block are processed before stopping, and retained historical snapshots are
  emitted only after a block is fully processed.
  Postgres-backed sync holds a session-level advisory lock for the selected
  deployment, so a second indexer cannot race the same instance.
- `sync`, `serve`, and `compare` support `--storage json` and
  `--storage postgres`. The Postgres backend stores current-state deployments,
  entities, dynamic source instances, processed-log cursors, historical
  checkpoints, and compact entity-version deltas in normalized tables, then
  reconstructs the same `StoreSnapshot` used by the query engine.
- Postgres also stores the shared raw chain feed: feed subscriptions, raw
  blocks, and raw logs keyed by `chain_id`. `sync --log-source postgres-feed`
  consumes the feed, while `sync --log-source rpc` keeps the direct RPC path.
- `chain-reader` owns RPC polling for one `chain_id` and writes raw logs for
  all registered subscriptions on that chain. Passing a manifest registers its
  static data source subscriptions. When no explicit RPC is configured,
  `chain-reader` tries resolved Chainlist URLs in order. Before appending new
  logs, it checks stored feed cursor hashes against the selected RPC and rolls
  the chain feed back from the first mismatched block by pruning raw
  blocks/logs and rewinding affected subscription cursors.
- `deploy --provider local` registers feed subscriptions, runs bounded
  chain-reader/sync passes for `postgres-feed`, and only succeeds once dynamic
  data source subscriptions created by mappings are backfilled and the
  checkpoint is complete.
- `serve` reloads the selected store on each GraphQL/health request so API
  containers see Postgres writes from indexer containers without restart.
- `serve` exposes `/graphql` plus GraphiQL. `/` and `/status` render the public
  brutalist status page. The sync log is paginated by `sync_page`/`sync_limit`,
  hides empty blocks by default, supports `show_empty=1`, and links block rows
  to the configured explorer through `UGRAPH_CHAIN_ID` or
  `UGRAPH_BLOCK_EXPLORER_URL`.
  Current query support covers POST/GET `/graphql`, CORS preflight,
  `operationName`, variables, `_meta.block.number`, `hasIndexingErrors`,
  entity lookup by ID, plural entity lists, `where`, nested direct relations,
  `@derivedFrom`, `first`, `skip`, `orderBy`, `orderDirection`, named
  fragments, inline fragments, `@include`, `@skip`, scalar output,
  `_meta.block.hash` for the current snapshot, and generated schema
  introspection for entity/filter/meta types. Exact GraphQL validation/error
  parity is still incomplete.
- GraphiQL uses pinned React/GraphiQL assets and falls back to a built-in query
  UI if external assets do not load.
- A fixed-block smoke diff against Goldsky `growfi/4.0.2` at block `10846000`
  matched through `ugraph compare` for `_meta(block:)`, `campaigns(block:,
  where: id_in, orderBy:)`, `acceptedTokens`, `purchases`, and
  `purchase.campaign`. The latest smoke query also covered named fragments,
  inline fragments, `@include`, and `@skip`.
- `ugraph conformance` runs batch hosted-provider diffs from JSON case files.
  The GrowFi fixture cases live at `examples/growfi/conformance.json`.
- `ugraph matrix` is the repeatable compatibility gate: it runs structural
  `doctor`, optional bounded sync when `--to-block` is provided, and optional
  GraphQL conformance when both `--endpoint` and `--cases-file` are provided.
  It emits one JSON/text report with structural, sync, conformance, and notes
  sections.
- Live Sepolia buy smoke: transaction
  `0x0ce83b9006ae4a7ce985505f6eee0e52b54d9ed07a0f0c4d76bee95bb1df3c25`
  at block `10866837` bought 1 MockUSDC on Olive Sicily. Incremental sync from
  the previous snapshot executed 4 logs with 0 validation errors in 8 seconds
  wall clock including rebuild, local `/graphql` returned the purchase, and
  `ugraph compare` matched Goldsky exactly.
- Local dynamic-source deploy smoke over Sepolia block `10845895` completed in
  2 passes using `https://sepolia.drpc.org`: pass 1 executed 2 static logs and
  created 3 dynamic sources, pass 2 backfilled 10 total subscriptions and
  closed the checkpoint with 0 validation errors. The publicnode Sepolia
  endpoint timed out on some `eth_getLogs` calls during this smoke.
- Chainlist fallback smoke with no explicit RPC also read Sepolia block
  `10845895`, registered 7 subscriptions, and inserted 2 logs after skipping
  bad public endpoints.
- Raw feed reorg smoke over Sepolia block `10845895` passed: after manually
  corrupting subscription cursor hashes, `chain-reader` rolled back from that
  block, deleted 1 raw block and 2 raw logs, rewound 7 subscriptions, and
  reinserted the 2 canonical logs.
- Uniswap v3 mainnet stress fixture: official `Uniswap/v3-subgraph` builds with
  `graph-cli`, `ugraph doctor` and `compat` pass for 1 static data source, 1
  template, 2 WASM modules, 6 handlers, and 20 imported host exports with no
  missing host exports. A 10,000-block `PoolCreated` scan from block `12369621`
  found 406 logs in 11.65 seconds against `ethereum-rpc.publicnode.com`. A
  first-log sync produced the expected UNI/WETH pool, token names, 5 entities,
  1 dynamic source, and 0 validation errors in 7.98 seconds. Before decimal128
  precision limiting, a 25-log stress sync across the first 1,000 blocks took
  131.94 seconds. After BigDecimal precision limiting, pow10 caching, a shared
  HTTP client, and per-run `eth_call` result caching, the same 25-log stress
  sync took 14.96 seconds with the same 85 entities, 7 dynamic sources, and 0
  validation errors. A full 1,000-block pass processed all 85 available logs in
  27.27 seconds, producing 254 entities, 17 dynamic sources, and 0 validation
  errors. After per-run WASM module caching, the same 25-log first-1,000-block
  stress slice in `--release` completed in 9.27 seconds wall clock with 85
  entities, 7 dynamic sources, and 0 validation errors.
- Aave v3 mainnet fixture: official `aave/protocol-subgraphs` builds with
  modern `graph-cli` for `VERSION=v3 BLOCKCHAIN=v3 NETWORK=mainnet`. It has 3
  static data sources, 8 templates, 8 compiled WASM modules, 73 event handlers,
  and 21 required host imports with no missing host exports. `abi-events`
  passes after supporting Hardhat-style artifact JSON. `doctor` intentionally
  reports a handler export/signature failure because the official manifest
  declares `PoolConfigurator.handleReserveActive` while the compiled mapping
  exports `handleReserveActivated`; keep this as an upstream fixture mismatch
  that ugraph correctly detects. Runtime replay over blocks
  `16291006..16292006` against `ethereum-rpc.publicnode.com` executed 12 logs,
  created 3 dynamic sources (`PoolAddressesProvider`, `Pool`,
  `PoolConfigurator`), and produced 0 validation errors. `sync` over the same
  range completed in 6.92 seconds wall clock in debug mode and 4.71 seconds in
  `--release` with 5 entities, 3 dynamic sources, complete checkpoint
  `16292006`, and 0 validation errors. Local `/healthz` and `/graphql` served
  that snapshot successfully. `ugraph matrix` reports `structural=false` for
  the upstream handler mismatch and `sync.ok=true` for this slice.
- Compound v2 official subgraph was attempted as a legacy fixture. The manifest
  uses `apiVersion: 0.0.3`; modern `graph-cli` rejects mappings below
  `0.0.5`, while old CLI versions currently fail during install/build from
  historical dependencies. Treat this as a legacy build-tooling blocker rather
  than a runtime compatibility result.
- BAYC IPFS fixture: `syamantak01/BoredApeYachtClub-API` uses
  `ipfs.cat` plus `json.fromBytes` in `src/token.ts` to load NFT metadata from
  `QmeSjSinHpPnmXmspMjwiXyN6zS4E9zccariGR3jxcaWtq/<tokenId>`. The cloned
  fixture at `/private/tmp/ugraph-bayc-ipfs` includes a built WASM. `doctor`
  passes with 1 data source, 1 handler, 1 WASM, 8 required host imports, and no
  missing host exports. Matrix over mainnet block `12292922` with
  `UGRAPH_IPFS_GATEWAY=https://dweb.link/ipfs/` executed 30 Transfer logs,
  produced 31 entities, and had 0 validation errors. The default `ipfs.io`
  gateway failed in this network due TLS certificate validation; `dweb.link`
  returned metadata successfully.
- Call AssemblyScript `_start` after WASM instantiation; graph-ts globals rely
  on it.
- Implement full Graph host exports, entity store semantics, dynamic data
  sources, and GraphQL query compatibility here before wiring infra.

## Storage

Postgres is the canonical production store target. The implemented storage
backends are durable JSON snapshots and transactional Postgres current-state
tables plus compact retained history tables. SQLite is only for local
development and focused tests. Redis is out of scope.

## Containers

The single Docker image is mode-driven:

- `UGRAPH_MODE=serve` runs the GraphQL API.
- `UGRAPH_MODE=indexer` runs `sync --watch`.
- `UGRAPH_MODE=chain-reader` runs the shared raw feed reader.
- `UGRAPH_LOG_SOURCE=rpc|postgres-feed` chooses direct RPC sync or local feed
  sync.
- `docker-compose.yml` starts Postgres, chain-reader, feed-backed indexer, and
  API for local production smoke tests.
- `UGRAPH_REORG_POLICY=fail|rollback|reset` controls checkpoint hash mismatch
  behavior. `rollback` probes retained checkpoints and rewinds to the newest
  matching block.
- `UGRAPH_REORG_CHECK_DEPTH` bounds rollback checkpoint probes.
- `UGRAPH_HISTORY_LIMIT` bounds retained current-state historical snapshots;
  `0` keeps all retained snapshots.
- `UGRAPH_MAX_BLOCK_RANGE` and `UGRAPH_RPC_RETRIES` harden RPC log scanning.
- `UGRAPH_RPC_TIMEOUT_SECS` bounds individual RPC and registry HTTP requests.
- The API exposes `/status` for an HTML operator view, `/healthz` for
  readiness, and `/metrics` for Prometheus.

## Quality Gate

- Run `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`,
  and `cargo test` before treating core changes as ready.
