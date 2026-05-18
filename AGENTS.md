# ugraph Working Notes

## Project Shape

- `core/` is the canonical Rust workspace. It contains the Graph Node/Goldsky-compatible runtime, manifest parser, ABI checks, WASM checks, RPC scanner, CLI, docs, and compatibility fixtures.
- `infra/` is reserved for taking `core` online: Docker images, Cloud Run service definitions, managed database wiring, secrets, observability, and deployment scripts.
- The project must stay agnostic. GrowFi under `core/examples/growfi/` is only a real fixture, never hardcoded business logic.

## Compatibility Target

- `core` must accept standard Graph Protocol subgraphs as-is: `subgraph.yaml`, `schema.graphql`, ABI files, templates, handlers, and compiled AssemblyScript WASM.
- Do not claim Goldsky equivalence until mapping execution, Graph host exports, entity store semantics, dynamic data sources, GraphQL query responses, and fixed-block output diffs are implemented.
- The compatibility gate starts with `cargo run -p ugraph -- doctor --manifest examples/growfi/subgraph.yaml` from `core/`.
- The runtime must call AssemblyScript `_start` after instantiation. graph-ts global/static constants are not valid before `_start`.
- The first real replay path is `cargo run -p ugraph -- replay --manifest examples/growfi/subgraph.yaml --rpc-url <rpc> --from-block <n> --to-block <n>` from `core/`.
- `replay` currently executes real compiled graph-ts handlers, decodes in-memory `store.set` entities, and keeps that entity store shared across replayed logs. The GrowFi `handleGrowfiContractsSet` fixture writes a decoded `Protocol` entity with Bytes fields from a live Sepolia log.
- `dataSource.create` is captured from graph-ts WASM as template name plus string params. `replay` instantiates those templates from the manifest, scans the created addresses, and queues their logs. A live GrowFi replay discovers `Campaign`, `StakingVault`, and `HarvestManager`, then executes `templates/Campaign/Campaign.wasm`.
- `dataSource.createWithContext` captures graph-ts `DataSourceContext`
  entities, persists them with dynamic source snapshots in JSON/Postgres, and
  restores them through `dataSource.context` when dynamic source handlers run.
- BigInt host exports implemented so far: `bigInt.plus`, `bigInt.minus`, `bigInt.times`, `bigInt.dividedBy`, and `bigInt.pow`.
- `bigInt.dividedByDecimal` is implemented for larger modern subgraphs such as
  Aave v3.
- BigDecimal host exports implemented so far: `bigDecimal.fromString`,
  `bigDecimal.plus`, `bigDecimal.minus`, `bigDecimal.times`,
  `bigDecimal.dividedBy`, `bigDecimal.equals`, and `bigDecimal.toString`.
  Values are clamped to decimal128-style 34 significant digits, matching The
  Graph's documented BigDecimal precision. Keep the Uniswap v3 zero-operation
  and large-exponent-gap shortcuts because tick math can create huge decimal
  scales that should not be rescaled for simple comparisons, subtracting zero,
  or precision-insignificant additions.
- `ethereum.call` is implemented generically for static ABI calls: it decodes graph-ts `SmartContractCall`, builds calldata from `functionSignature`, performs JSON-RPC `eth_call` at the handler block, and decodes static outputs. Historical replay requires an archive-capable RPC; do not silently fall back to `latest`.
- `ethereum.call` also decodes dynamic output heads/tails for `string` and
  `bytes`; ERC20 `name()` and `symbol()` calls from large real subgraphs depend
  on this. `typeConversion.stringToH160` is implemented for address conversion
  from graph-ts strings.
- `typeConversion.bytesToString` trims trailing null bytes for fixed bytes, so
  graph-ts `bytes32.toString()` comparisons work for IDs like `POOL` and
  `POOL_CONFIGURATOR`.
- `json.fromBytes`, `json.try_fromBytes`, JSON numeric conversions,
  `ipfs.cat`, `ipfs.getBlock`, and `ipfs.map` are implemented for file-backed
  subgraphs. IPFS gateway behavior is configured with `UGRAPH_IPFS_GATEWAY`,
  `UGRAPH_IPFS_TIMEOUT_SECS`, and `UGRAPH_MAX_IPFS_FILE_BYTES`; `ipfs.map`
  follows Graph Node's JSON-line behavior: each non-empty line is parsed as one
  `JSONValue` and passed to the exported WASM callback.
- ABI validation accepts both raw ABI arrays and compiler artifact JSON with a
  top-level `abi` field. Schema parsing handles multiline `@derivedFrom`
  directives.
- `core serve` now supports POST/GET `/graphql`, CORS preflight, GraphiQL,
  `operationName`, variables, `_meta.block.number`, `hasIndexingErrors`,
  entity lookup, plural lists, `where`, nested direct relations,
  `@derivedFrom`, ordering, pagination, named fragments, inline fragments,
  `@include`, `@skip`, scalar output, `_meta.block.hash` for current snapshots,
  and generated schema introspection for entity/filter/meta types.
- `core serve` supports retained historical current-state selection through
  root `block: { number }` and `block: { hash }` arguments.
- `core sync`, `core serve`, and `core compare` support both JSON snapshots and
  Postgres current-state storage through `--storage postgres --deployment <id>
  --postgres-url <url>`. Postgres tables are normalized for deployments,
  entities, dynamic sources, and processed-log cursors.
- `core sync --watch` is the live indexer loop. It repeats the current-state
  sync pass after `UGRAPH_POLL_INTERVAL_MS`, logs transient failures as JSON,
  and retries with capped exponential backoff.
- `core replay/sync` uses a per-run WASM module cache so each distinct mapping
  WASM is compiled once, then instantiated per log. Handler writes run against
  candidate store/cache state and commit only after schema validation passes,
  so invalid `store.set` payloads do not mutate the run store or spawn dynamic
  sources. The `--limit` value is a soft cap at block boundaries: once a block
  starts, all remaining logs in that block are processed before stopping, and
  retained history snapshots are only emitted after complete blocks.
- `core sync` retains historical snapshots via `UGRAPH_HISTORY_LIMIT`; `0`
  keeps all retained snapshots. Postgres stores retained checkpoints and
  compact entity-version deltas in dedicated history tables.
- `core scan/sync` chunks `eth_getLogs` with `UGRAPH_MAX_BLOCK_RANGE`, retries
  transient RPC failures with `UGRAPH_RPC_RETRIES`, and splits range-limit
  failures recursively.
- `core sync` checks the stored checkpoint block hash before resuming.
  `UGRAPH_REORG_POLICY=fail|rollback|reset` controls mismatch behavior.
  `rollback` probes retained checkpoints up to `UGRAPH_REORG_CHECK_DEPTH` and
  rewinds to the newest matching block.
- `core sync` holds a Postgres advisory lock for the selected deployment so
  two indexers cannot write the same single-subgraph instance concurrently.
- `core serve` reloads the selected store on each GraphQL/health request, so
  API containers see indexer writes committed to Postgres without restart.
- `core serve` exposes `/status` as a small HTML operational page and
  `/metrics` in Prometheus text format.
- `core conformance` runs batch GraphQL diffs against Goldsky/Graph Node from
  JSON case files. GrowFi cases live in `core/examples/growfi/conformance.json`.
- `core matrix` is the repeatable compatibility report command. It runs
  structural `doctor`, optional bounded sync when `--to-block` is provided, and
  optional GraphQL conformance when both `--endpoint` and `--cases-file` are
  provided. Use it for fixture reports instead of ad hoc command bundles.
- `core` has a single Docker image controlled by
  `UGRAPH_MODE=serve|indexer|chain-reader` and a local `docker-compose.yml`
  with Postgres, a shared chain reader, a feed-backed indexer, and API.
- `core chain-reader` registers static manifest subscriptions when a manifest
  is provided, reads raw logs for one `chain_id`, and writes them into
  Postgres feed tables. `core sync --log-source postgres-feed` consumes that
  feed instead of calling `eth_getLogs` directly. Direct RPC sync remains
  available with `--log-source rpc`.
- `core deploy --provider local` registers feed subscriptions, runs a bounded
  chain-reader pass when using `postgres-feed`, syncs the deployment, and
  reports feed/sync status.
- Core readiness requires `cargo fmt`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test`.
- A fixed-block smoke diff against Goldsky `growfi/4.0.2` at block `10846000`
  matched through `ugraph compare` for `_meta(block:)`, `campaigns(block:,
  where: id_in, orderBy:)`, `acceptedTokens`, `purchases`, and
  `purchase.campaign`. The current smoke query also covers named fragments,
  inline fragments, `@include`, and `@skip`.
- Uniswap v3 mainnet stress fixture passed structural compatibility using the
  official `Uniswap/v3-subgraph`: `doctor` and `compat` reported 1 static data
  source, 1 template, 2 WASM modules, 6 handlers, and 20 imported host exports
  with no missing host exports. A 10,000-block `PoolCreated` scan from block
  `12369621` found 406 logs in 11.65 seconds. A first-log sync produced the
  expected UNI/WETH pool with correct token metadata, 5 entities, 1 dynamic
  source, and 0 validation errors in 7.98 seconds. BigDecimal precision
  limiting, pow10 caching, shared HTTP client reuse, and per-run `eth_call`
  caching reduced the first-1,000-block 25-log stress sync from 131.94 seconds
  to 14.96 seconds with identical entity/dynamic-source counts and 0 validation
  errors. A complete first-1,000-block pass processed all 85 available logs in
  27.27 seconds, producing 254 entities, 17 dynamic sources, and 0 validation
  errors. With per-run WASM module caching, the same 25-log first-1,000-block
  stress slice in `--release` completed in 9.27 seconds wall clock with 85
  entities, 7 dynamic sources, and 0 validation errors.
- Aave v3 mainnet fixture builds from official `aave/protocol-subgraphs`
  (`VERSION=v3 BLOCKCHAIN=v3 NETWORK=mainnet`) with 3 static data sources, 8
  templates, 8 compiled WASM modules, 73 handlers, and 21 required host imports
  with no missing host exports. `abi-events` passes. `doctor` still reports the
  official manifest mismatch where `PoolConfigurator` declares
  `handleReserveActive` but the WASM exports `handleReserveActivated`. Runtime
  replay over mainnet blocks `16291006..16292006` executed 12 logs, discovered
  `PoolAddressesProvider`, `Pool`, and `PoolConfigurator` dynamic sources, and
  produced 0 validation errors. `sync` for the same range completed in 6.92
  seconds in debug mode and 4.71 seconds in `--release` with 5 entities, 3
  dynamic sources, complete checkpoint `16292006`, and local `/healthz` plus
  `/graphql` served the snapshot. `core matrix` reports this fixture as
  structurally false because of the upstream handler mismatch, but with
  `sync.ok=true` for the tested slice.
- Compound v2 official is a legacy tooling fixture: its manifest uses
  `apiVersion: 0.0.3`, modern `graph-cli` refuses mappings below `0.0.5`, and
  older CLI versions currently fail from historical install/build dependencies.
- BAYC IPFS fixture: `syamantak01/BoredApeYachtClub-API` uses `ipfs.cat` plus
  `json.fromBytes` to load NFT metadata from IPFS. The clone at
  `/private/tmp/ugraph-bayc-ipfs` includes built WASM. `doctor` passes with 8
  required host imports and no missing exports. Matrix over mainnet block
  `12292922` with `UGRAPH_IPFS_GATEWAY=https://dweb.link/ipfs/` executed 30
  Transfer logs, produced 31 entities, and had 0 validation errors. The default
  `ipfs.io` gateway failed in this local network due TLS certificate
  validation.

## Storage Decision

- Postgres is the primary store target. It matches Graph Node's production model best and supports indexes, transactions, block-scoped commits, rollback/reorg handling, derived relationships, ordering, and filtering.
- Current implementation has transactional Postgres current-state storage,
  retained compact history tables, advisory indexer locking, and checkpoint
  rollback for reorgs.
- Current implementation also has shared feed tables for subscriptions, raw
  blocks, and raw logs keyed by `chain_id`.
- Live Sepolia buy smoke passed: tx
  `0x0ce83b9006ae4a7ce985505f6eee0e52b54d9ed07a0f0c4d76bee95bb1df3c25`,
  block `10866837`, 4 logs executed, 0 validation errors, local GraphQL matched
  Goldsky exactly for the new purchase.
- Redis is out of scope for now.
- MongoDB is not the primary compatibility path. It can be explored behind a store adapter later, but only if it can prove equivalent behavior for GraphQL filters, ordering, relationships, historical block semantics, and atomic block commits.
- SQLite can be used for local development and small tests, not as the production compatibility target.

## Query UI

- The classic hosted-subgraph UI is GraphiQL. `infra` should expose GraphiQL next to the GraphQL endpoint once the query server exists.
- The public query endpoint should accept normal GraphQL envelopes and match hosted subgraph response shapes, including `_meta`.

## Deployment Direction

- Build container-first. A single Docker image should run locally and deploy cleanly to any container host.
- DigitalOcean with managed Postgres is a likely target; do not bake in Cloud Run assumptions.
- Keep `core` runnable without cloud services for local compatibility tests.
- Production should use a shared multi-chain feed: one `chain-reader` per
  `chain_id` reads RPC once and stores raw blocks/logs in Postgres;
  deployment-specific sync workers consume matching raw logs from that feed and
  write isolated entity stores under separate `UGRAPH_DEPLOYMENT` ids.
- A deployment may subscribe to multiple `chain_id` values. Keep every raw feed
  table, cursor, subscription, and entity write scoped by chain/deployment where
  appropriate.
- The deploy UX should be a single CLI call (`ugraph deploy ...`) that creates
  or reuses shared infrastructure, ensures the required chain readers exist,
  registers the subgraph deployment, runs sync, and exposes GraphQL/GraphiQL.
  Avoid making operators manually wire Cloud Run services/jobs/schedulers per
  subgraph.
