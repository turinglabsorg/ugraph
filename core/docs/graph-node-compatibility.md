# Graph Node Compatibility Plan

This document tracks what `ugraph` must replicate to be genuinely compatible
with Graph Node, Goldsky, and other hosted subgraph providers.

## Compatibility Target

`ugraph` should run an existing Graph Protocol subgraph without proprietary
rewrite:

- `subgraph.yaml` is accepted as-is.
- `schema.graphql` is accepted as-is.
- ABI files are accepted as-is.
- AssemblyScript mappings compiled by `graph-cli` are executed as WASM.
- Dynamic data source templates work through `dataSource.create`.
- Queries are served through a GraphQL endpoint with the same entity schema
  semantics.

Anything less is only "subgraph-like", not compatible.

## What Graph Node Actually Does

Official Graph Node docs split indexing into three stages:

1. Fetch events of interest from the provider.
2. Process events in order with the appropriate handlers.
3. Write resulting data to the store.

The same docs describe Graph Node as a Rust service backed by PostgreSQL and
IPFS, with public query and private admin/status/metrics surfaces.

For `ugraph`, the minimum equivalent path is:

1. Load the manifest and schema.
2. Build data source cursors from `startBlock`.
3. Fetch logs in deterministic block/log order.
4. Decode triggers from ABI signatures.
5. Instantiate the compiled mapping WASM, run AssemblyScript `_start`, and
   execute the matching handler.
6. Expose Graph-compatible host exports to the WASM module.
7. Commit entity changes atomically per block.
8. Expose GraphQL reads over the indexed entity store.

## Host Export Surface

Graph Node's `runtime/wasm/src/module/instance.rs` explicitly registers host
exports. `ugraph` needs these groups before we can claim runtime compatibility:

- Chain calls: `ethereum.call`, `ethereum.getBalance`, `ethereum.hasCode`
- ABI helpers: `ethereum.encode`, `ethereum.decode`
- Store: `store.get`, `store.get_in_block`, `store.set`, `store.remove`,
  `store.loadRelated`
- Dynamic data sources: `dataSource.create`,
  `dataSource.createWithContext`, `dataSource.address`,
  `dataSource.network`, `dataSource.context`
- Files: `ipfs.cat`, `ipfs.map`, `ipfs.getBlock`
- Conversion: `typeConversion.*`
- JSON/YAML: `json.*`, `yaml.*`
- Crypto: `crypto.keccak256`
- Numerics: `bigInt.*`, `bigDecimal.*`
- ENS/logging: `ens.nameByHash`, `log.log`
- Runtime control: `abort`, `gas`

The first GrowFi-compatible milestone can ship a smaller implemented subset
only if the importer verifies that the compiled WASM does not import the
missing functions.

Current runtime progress:

- `ugraph replay` can scan a live Sepolia block, decode the ABI event, allocate
  a graph-ts-compatible `EthereumEvent_0_0_7`, and call the compiled
  AssemblyScript handler.
- The GrowFi fixture executes `handleGrowfiContractsSet` from the real
  `CampaignFactory.wasm`; the observed host calls are `store.get`,
  `store.set`, `typeConversion.bytesToHex`, and `log.log`.
- `store.set` decodes graph-ts `Entity` typed maps into an in-memory entity
  store, and `store.get` can rehydrate those values back into WASM memory.
  The live Sepolia GrowFi replay writes a decoded `Protocol` entity with
  `Bytes` fields for `growToken`, `growMinter`, `growTreasury`, and
  `growFeeSplitter`.
- `replay` shares one in-memory entity store across executed logs, so later
  handlers can observe earlier writes during the same replay run.
- `dataSource.create` is decoded from graph-ts WASM as a template name plus
  string params. The GrowFi `handleCampaignCreated` fixture captures the
  `Campaign`, `StakingVault`, and `HarvestManager` template spawns.
- `replay` instantiates those dynamic sources from the manifest templates,
  scans their addresses, queues their matching logs, and executes template WASM
  from `build/templates/<Template>/<Template>.wasm`.
- `dataSource.createWithContext` captures graph-ts `DataSourceContext`
  entities, persists them with dynamic source snapshots in JSON/Postgres, and
  restores them through `dataSource.context` for dynamic source handlers.
- BigInt host arithmetic is implemented for `plus`, `minus`, `times`,
  `dividedBy`, `dividedByDecimal`, and `pow`.
- BigDecimal host arithmetic is implemented for `fromString`, `plus`, `minus`,
  `times`, `dividedBy`, `equals`, and `toString`. Values are clamped to
  decimal128-style 34 significant digits, matching The Graph's documented
  BigDecimal precision. Uniswap v3 showed that tick math can create very large
  decimal scales, so the runtime avoids unnecessary rescaling for zero
  comparisons, zero subtraction, and precision-insignificant exponent gaps.
- `ethereum.call` decodes graph-ts `SmartContractCall`, builds calldata from
  the generated `functionSignature`, performs JSON-RPC `eth_call` at the
  handler block, and decodes static ABI outputs plus dynamic `string` and
  `bytes` outputs. Public non-archive RPCs may return historical-state errors;
  replay treats those as reverted calls instead of falling back to `latest`.
- `typeConversion.stringToH160` is implemented for graph-ts address conversion
  from strings.
- `typeConversion.bytesToString` trims trailing null bytes for right-padded
  fixed bytes, which is required by subgraphs that compare `bytes32.toString()`
  values such as Aave v3 component IDs.
- JSON host exports are implemented for `json.fromBytes`,
  `json.try_fromBytes`, `json.toI64`, `json.toU64`, `json.toF64`, and
  `json.toBigInt`.
- IPFS host exports are implemented for `ipfs.cat`, `ipfs.getBlock`, and
  `ipfs.map`. Fetches use `UGRAPH_IPFS_GATEWAY` with optional `{path}`
  placeholder plus timeout and size guards. `ipfs.map` follows Graph Node's
  JSON-line behavior: each non-empty line is parsed as one graph-ts `JSONValue`
  and passed to the mapping's exported callback.
- ABI event validation accepts raw ABI arrays and compiler artifact JSON files
  with a top-level `abi` field.
- `schema.graphql` is parsed into entity/field metadata, and runtime
  `store.set` payloads are validated for entity type, required fields, scalar
  types, `id` consistency, unknown fields, and `@derivedFrom` write attempts.
  Multiline `@derivedFrom` directives are recognized as derived fields.
- `sync` persists the current entity store, schema, checkpoint, dynamic source
  instances, and partial-run processed-log cursors to either a durable JSON
  snapshot or a transactional Postgres current-state deployment. A later `sync`
  resumes from the checkpoint and continues scanning persisted dynamic sources
  without dropping unprocessed logs.
- `sync --watch` polls continuously and repeats the same deterministic
  current-state pass for live indexing.
- Replay/sync uses a per-run compiled WASM module cache while preserving fresh
  per-log instances and host state. Handler writes run against candidate
  store/cache state and commit only after schema validation passes, so invalid
  `store.set` payloads do not mutate the run store or spawn dynamic sources.
  `--limit` is a soft cap at block boundaries, so a started block is finished
  before stopping and historical snapshots are retained only after complete
  blocks.
- `serve` loads the selected current-state store and exposes `/graphql` with
  GraphQL envelopes plus a GraphiQL UI.

## Store Semantics

Graph Node is not just a key-value store. Compatibility requires:

- Entity type validation against `schema.graphql`.
- Entity access restrictions from each data source `mapping.entities`.
- Block-scoped writes and deterministic handler failure behavior.
- Historical block awareness for `_meta`, block pointers, and reorg handling.
- Derived relationship resolution for `@derivedFrom`.
- ID and scalar encoding compatible with GraphQL entity responses.

The implemented current-state stores are JSON snapshots and Postgres
deployments. Postgres stores normalized rows for deployment metadata, entities,
dynamic sources, and processed-log cursors, then reconstructs the same
`StoreSnapshot` used by the query engine. The store API must keep moving toward
Graph Node semantics rather than a generic JSON cache.

## Dynamic Data Sources

Templates are non-negotiable for GrowFi. The GrowFi fixture has three
templates. Graph-compatible behavior means:

- A mapping calls `dataSource.create(name, params)`.
- The runtime captures the matching template name and params from WASM.
- `replay` resolves the template from `subgraph.yaml`, uses the first param as
  the Ethereum contract address, scans from the creation block, and adds those
  logs to the ordered replay queue.
- Dynamic source instances are persisted in the current-state snapshot so
  indexing can resume across process restarts. JSON and Postgres storage both
  persist them.
- Future blocks are scanned for that new source from the creation block.

## Query API

GraphQL compatibility requires:

- Entity queries by ID.
- Plural entity queries with `where`, `orderBy`, `orderDirection`, `first`,
  `skip`.
- Nested entity selection.
- Named fragments, inline fragments, `@include`, and `@skip`.
- `_meta { block { number hash } hasIndexingErrors }`.
- Graph scalar output conventions for `Bytes`, `BigInt`, `BigDecimal`, `ID`.
- Introspection that exposes generated entity, filter/input, and meta types.

GrowFi's current fixture is a useful acceptance test because it exercises
entities, nested relations, `_meta`, ordering, filtering, and dynamic
templates.

Current `serve` support covers POST and GET `/graphql`, CORS preflight,
GraphiQL, `operationName`, variables, `_meta.block.number`,
`hasIndexingErrors`, entity-by-ID lookups, plural entity lists, `where`
filters, nested direct relations, `@derivedFrom` relations, `first`, `skip`,
`orderBy`, `orderDirection`, named fragments, inline fragments, `@include`,
`@skip`, scalar output for stored values, `_meta.block.hash` for the current
snapshot, and generated schema introspection for entity/filter/meta types.
Remaining query compatibility work is aggregate query features, historical
store reads, and exact GraphQL validation/error semantics.

## Verification Strategy

The equivalence test should compare `ugraph` and Goldsky responses for the same
deployment and block range:

1. Build the GrowFi subgraph with `graph-cli`.
2. Load its compiled WASM and manifest in `ugraph`.
3. Backfill from the manifest `startBlock` to a fixed target block.
4. Query Goldsky and `ugraph` with the same GraphQL documents.
5. Normalize JSON object key ordering only.
6. Diff values exactly.

`ugraph matrix` is the repeatable gate for this workflow. It emits one report
that includes structural `doctor`, optional bounded sync, optional GraphQL
conformance, and notes for skipped sections.

Baseline queries:

- `_meta`
- `globalStats`
- `campaigns`
- `campaign(id)`
- `purchases`
- `seasons`
- `positions`
- `claims`
- `producers`

Current fixed-block smoke:

- Goldsky endpoint:
  `https://api.goldsky.com/api/public/project_cmo1ydnmbj6tv01uwahhbeenr/subgraphs/growfi/4.0.2/gn`
- Block: `10846000`
- Query coverage: `_meta(block:)`, `campaigns(block:, where: id_in, orderBy:)`,
  nested `acceptedTokens`, nested `purchases`, and `purchase.campaign`.
- Result: `ugraph compare` normalized JSON matched the synced snapshot against
  Goldsky. The latest compare query also covers named fragments, inline
  fragments, `@include`, and `@skip`.

Current live transaction smoke:

- Transaction:
  `0x0ce83b9006ae4a7ce985505f6eee0e52b54d9ed07a0f0c4d76bee95bb1df3c25`
- Block: `10866837`
- Action: 1 MockUSDC buy on Olive Sicily
  `0x59e24007A065eB99c8C8C0325287E359eA4F41de`.
- Sync result from the previous current-state snapshot: 4 executed logs, 0
  validation errors, 8 seconds wall clock including a rebuild.
- Result: local `/graphql` returned the new `Purchase`, and `ugraph compare`
  matched Goldsky exactly for the purchase and updated campaign aggregates.

Current Uniswap v3 stress fixture:

- Source: official `Uniswap/v3-subgraph` built with `graph-cli` for Ethereum
  mainnet.
- Manifest shape: factory data source
  `0x1F98431c8aD98523631AE4a59f267346ea31F984`, start block `12369621`, and
  one `Pool` template.
- Compatibility checks: `ugraph doctor`, `compat`, `handler-exports`, and
  `handler-signatures` pass for 2 WASM modules, 6 handlers, and 20 required
  host imports with no missing host exports.
- Scan result: 10,000 blocks from `12369621` to `12379621` found 406
  `PoolCreated` logs in 11.65 seconds against `ethereum-rpc.publicnode.com`.
- First-log sync result: 5 entities, 1 dynamic source, 1 executed log, and 0
  validation errors in 7.98 seconds. Local GraphQL returned the expected
  UNI/WETH pool with correct ERC20 `name`, `symbol`, and `decimals` values.
- Initial stress sync result: 25 executed logs across the first 1,000 blocks
  produced 85 entities, 7 dynamic sources, and 0 validation errors in 131.94
  seconds.
- Optimized stress sync result: BigDecimal precision limiting, pow10 caching,
  shared HTTP client reuse, and per-run `eth_call` result caching reduced that
  same 25-log run to 14.96 seconds with the same 85 entities, 7 dynamic sources,
  and 0 validation errors.
- Complete first-1,000-block result: all 85 available logs processed in 27.27
  seconds, producing 254 entities, 17 dynamic sources, and 0 validation errors.
- Release-mode cached-module result: the same 25-log first-1,000-block stress
  slice completed in 9.27 seconds wall clock with 85 entities, 7 dynamic
  sources, and 0 validation errors.
- Matrix result: structurally and synchronously OK for the tested 25-log
  first-1,000-block slice.

Current Aave v3 mainnet fixture:

- Source: official `aave/protocol-subgraphs`, prepared with
  `VERSION=v3 BLOCKCHAIN=v3 NETWORK=mainnet`.
- Manifest shape: 3 static data sources, 8 templates, 8 compiled WASM modules,
  and 73 event handlers.
- Compatibility checks: `compat` reports 21 required host imports and no
  missing host exports. `abi-events` passes after supporting compiler artifact
  ABI JSON. `doctor` still reports one upstream handler mismatch because the
  manifest declares `PoolConfigurator.handleReserveActive` while the compiled
  WASM exports `handleReserveActivated`.
- Runtime replay result: blocks `16291006..16292006` against
  `ethereum-rpc.publicnode.com` executed 12 logs, created 3 dynamic sources
  (`PoolAddressesProvider`, `Pool`, and `PoolConfigurator`), and produced 0
  validation errors.
- Sync result: the same range completed in 6.92 seconds wall clock, producing
  5 entities, 3 dynamic sources, complete checkpoint `16292006`, and 0
  validation errors. Release mode with per-run WASM module caching completed
  the same slice in 4.71 seconds wall clock.
- API result: local `/healthz` reported the complete Aave snapshot and local
  `/graphql` returned `_meta`, `pools`, and `priceOracles` from that snapshot.
- Matrix result: `structural=false` because of the upstream manifest/export
  mismatch, while `sync.ok=true` for the tested slice.

Current Compound v2 legacy fixture:

- Source: official `compound-finance/compound-v2-subgraph`.
- Blocker: the manifest uses `apiVersion: 0.0.3`; modern `graph-cli` rejects
  mappings below `0.0.5`, and older CLI versions currently fail from historical
  install/build dependencies. Keep this as a legacy build-tooling blocker, not
  a runtime compatibility result.

Current BAYC IPFS fixture:

- Source: `syamantak01/BoredApeYachtClub-API`, cloned at
  `/private/tmp/ugraph-bayc-ipfs`.
- Mapping behavior: `src/token.ts` calls `ipfs.cat` for
  `QmeSjSinHpPnmXmspMjwiXyN6zS4E9zccariGR3jxcaWtq/<tokenId>` and parses the
  bytes with `json.fromBytes` to populate image and trait fields.
- Compatibility checks: `doctor` passes for 1 static data source, 1 handler,
  1 WASM module, 8 required host imports, and no missing host exports.
- Runtime result: matrix over Ethereum mainnet block `12292922` with
  `UGRAPH_IPFS_GATEWAY=https://dweb.link/ipfs/` executed 30 Transfer logs,
  produced 31 entities, and produced 0 validation errors.
- Gateway note: the default `ipfs.io` gateway failed in this local network due
  TLS certificate validation; `dweb.link` returned the same metadata
  successfully.

The current code passes manifest, ABI, WASM import/export, handler signature,
live log scan, ABI decode, schema validation, dynamic template replay,
current-state sync, Postgres current-state roundtrip, GraphQL serving, and
GraphiQL smoke checks. Runtime equivalence still requires broader query diffs,
release-mode production-scale benchmarks for heavy subgraphs, and exact GraphQL
validation/error parity.
