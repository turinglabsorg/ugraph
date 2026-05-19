# ugraph Notes

## Architecture Updates

- The Rust workspace now lives at the repository root. `core/` owns libraries,
  fixtures, docs, and Docker runtime assets; `cli/` owns the `ugraph` binary.
  Run Cargo commands from the repository root and reference GrowFi fixtures as
  `core/examples/growfi/...`.
- Production uses a shared multi-chain feed, not one RPC scanner per subgraph.
- Run one `chain-reader` per `chain_id`. Each reader owns RPC polling for that
  chain and writes raw blocks/logs to Postgres.
- Raw feed tables, cursors, subscriptions, and replay boundaries must be keyed
  by `chain_id`.
- Deployment sync workers consume matching raw logs from Postgres and write
  isolated entity stores under their own `UGRAPH_DEPLOYMENT` ids.
- A deployment can subscribe to multiple `chain_id` values when a subgraph spans
  multiple chains.
- The live GrowFi fixture tracks the Sepolia `4.0.3` refresh from block
  `10838711`; reset the `growfi` deployment before switching away from older
  fixture versions so stale checkpoints do not resume past the new start block.
- The intended operator flow is `ugraph deploy ...`: create or reuse shared
  infrastructure, ensure required chain readers exist, register subscriptions,
  run sync, and expose GraphQL/GraphiQL.
- Implemented local flow: `ugraph deploy --provider local` can register feed
  subscriptions and loop bounded `chain-reader`/`sync` passes from
  `UGRAPH_LOG_SOURCE=postgres-feed` until dynamic data source backfills are
  complete.
- Implemented control-plane identity tables and CLI commands:
  `ugraph users` manages users, hashed API keys, and the public-signup switch;
  `ugraph deployments` lists ownership/version/visibility metadata and can
  update query visibility.
- `ugraph deploy --provider local` accepts `--version`, `--visibility
  private|public`, `--owner-email`, and `--api-key`. API keys require `deploy`
  scope for deploy metadata writes; private GraphQL deployments require a
  `query`-scoped key through `Authorization: Bearer <key>` or `x-api-key`.
- `ugraph deployments register` updates version/visibility/owner metadata for
  an existing deployment without running a sync. Deployment ids are unique in
  Postgres, so a name can only refer to one current deployment in a given
  instance.
- Implemented feed schema tables: `ugraph_feed_subscriptions`,
  `ugraph_raw_blocks`, and `ugraph_raw_logs`.
- Docker supports `UGRAPH_MODE=serve|indexer|chain-reader`. The entrypoint also
  forwards normal `ugraph` subcommands such as `deploy`, `chain-reader`, and
  `--help`.
- The public homepage is served from `/` and `/status` as a brutalist
  terminal-style status page with a single `made by turinglabs_` credit linked
  to `https://turinglabs.org`. It lists public deployment metadata and the
  append-only entity change timeline from `ugraph_entity_changes`: block,
  emitted timestamp, explorer link, and created/updated/removed entities.
  Entity-change audit rows include `previous_data`; the UI must render
  human-readable field summaries or before/after diffs instead of only raw
  entity IDs. Legacy no-op rows where `data` equals the previous entity state
  are pruned during migration. Blocks without entity changes are hidden by
  default, can be shown with `show_empty=1`, and the view is paginated with
  `sync_page` and `sync_limit`. This audit trail is separate from the retained
  history cache used for historical GraphQL queries and rollback. When the API gets
  `UGRAPH_CHAIN_ID` or `UGRAPH_BLOCK_EXPLORER_URL`, sync rows link blocks to
  the correct explorer and show emitted timestamps for newly written
  checkpoints. GraphiQL is served from pinned React/GraphiQL assets, uses
  in-memory editor storage to avoid stale browser query state, includes a
  built-in fallback query UI if external assets fail, and returns GraphQL-literal
  introspection defaults so GraphiQL can parse directive defaults cleanly.
- Hosted-provider query paths are supported at
  `/subgraphs/<deployment>/<version>/gn` and
  `/subgraphs/<deployment>/<version>/graphql`. `latest` aliases the current
  deployment; explicit versions must match registered deployment metadata.
- GraphQL selection validation now rejects unknown entity/meta fields instead
  of projecting them as `null`, matching Graph Node/Goldsky error behavior.
- `_meta.deployment` is exposed from the selected runtime deployment id, so
  hosted-provider meta queries can select it alongside `_meta.block`.
- `serve` keeps status endpoints lightweight: `/`, `/status`, `/healthz`, and
  `/metrics` read deployment counters/checkpoints without materializing
  retained history. GraphQL current-state queries also skip retained history;
  history is loaded only when the query contains a `block:` argument.
- RPC, Chainlist registry, and mapping `ethereum.call` requests are bounded by
  `UGRAPH_RPC_TIMEOUT_SECS`.
- Runtime execution enriches logs with RPC block metadata before calling WASM
  handlers, so Graph mappings receive real `event.block.timestamp` values
  instead of zero-filled block objects.
- When no explicit RPC is configured, `chain-reader` tries all resolved
  Chainlist URLs in order rather than pinning itself to the first URL.
- Shared feed reorg handling is implemented: `chain-reader` compares stored
  cursor hashes with the selected RPC and rolls back raw blocks/logs plus
  affected subscription cursors from the first mismatched block.
- Chainlist fallback smoke with no explicit RPC passed on Sepolia block
  `10845895`, registering 7 subscriptions and inserting 2 logs.
- Raw feed reorg smoke passed on Sepolia block `10845895`: after cursor hash
  corruption, `chain-reader` deleted 1 raw block and 2 raw logs, rewound 7
  subscriptions, and reinserted 2 canonical logs.
- Sepolia local smoke should prefer `https://sepolia.drpc.org`; the publicnode
  Sepolia endpoint timed out on some `eth_getLogs` calls during dynamic-source
  deploy testing.
- `UGRAPH_FROM_BLOCK` is the initial deployment start block only. Once a
  complete checkpoint exists, the watch indexer resumes from
  `checkpoint.to_block + 1`; use `--reset`/`UGRAPH_RESET=true` for an explicit
  rebuild from the configured start block. If the RPC head is equal to or
  behind the stored complete checkpoint, keep the previous checkpoint and do
  not write an empty inverted checkpoint range.
- API logs intentionally ignore client disconnects such as broken pipes; they
  are normal HTTP aborts, not indexing or GraphQL execution failures.
- Keep provider wiring out of the core runtime. DigitalOcean is a likely target,
  but the core container should stay portable.
- Lowest-cost deploy target is now `infra/gcp/e2-micro`: one Google Compute
  Engine `e2-micro` VM in an Always Free eligible region, local Docker Compose,
  local Postgres, and direct Docker image upload with no Cloud SQL, Cloud Run,
  or Artifact Registry dependency. Default profile is compact
  (`postgres` + direct-RPC `indexer` + `api`); the shared feed profile can be
  enabled later with `COMPOSE_PROFILES=feed` and
  `UGRAPH_LOG_SOURCE=postgres-feed`. Security default: Caddy terminates HTTPS
  on `ugraph.growfi.dev` when `DO_DNS_ZONE=growfi.dev` is set, or
  `<external-ip>.sslip.io` otherwise. The VM uses a dedicated `ugraph-net` VPC,
  not the GCP `default` network. Firewall opens only `80/443` publicly and SSH
  is restricted to the deploy operator IP. API/Postgres stay internal, the
  generated DB password is stored in
  `/opt/ugraph/.env` with `0600`, and startup enables unattended security
  updates plus a 2 GiB swapfile.
