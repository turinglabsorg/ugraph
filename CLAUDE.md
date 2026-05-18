# ugraph Notes

## Architecture Updates

- Production uses a shared multi-chain feed, not one RPC scanner per subgraph.
- Run one `chain-reader` per `chain_id`. Each reader owns RPC polling for that
  chain and writes raw blocks/logs to Postgres.
- Raw feed tables, cursors, subscriptions, and replay boundaries must be keyed
  by `chain_id`.
- Deployment sync workers consume matching raw logs from Postgres and write
  isolated entity stores under their own `UGRAPH_DEPLOYMENT` ids.
- A deployment can subscribe to multiple `chain_id` values when a subgraph spans
  multiple chains.
- The intended operator flow is `ugraph deploy ...`: create or reuse shared
  infrastructure, ensure required chain readers exist, register subscriptions,
  run sync, and expose GraphQL/GraphiQL.
- Implemented local flow: `ugraph deploy --provider local` can register feed
  subscriptions and loop bounded `chain-reader`/`sync` passes from
  `UGRAPH_LOG_SOURCE=postgres-feed` until dynamic data source backfills are
  complete.
- Implemented feed schema tables: `ugraph_feed_subscriptions`,
  `ugraph_raw_blocks`, and `ugraph_raw_logs`.
- Docker supports `UGRAPH_MODE=serve|indexer|chain-reader`. The entrypoint also
  forwards normal `ugraph` subcommands such as `deploy`, `chain-reader`, and
  `--help`.
- GraphiQL is served from pinned React/GraphiQL assets and includes a built-in
  fallback query UI if external assets fail.
- RPC, Chainlist registry, and mapping `ethereum.call` requests are bounded by
  `UGRAPH_RPC_TIMEOUT_SECS`.
- When no explicit RPC is configured, `chain-reader` tries all resolved
  Chainlist URLs in order rather than pinning itself to the first URL.
- Chainlist fallback smoke with no explicit RPC passed on Sepolia block
  `10845895`, registering 7 subscriptions and inserting 2 logs.
- Sepolia local smoke should prefer `https://sepolia.drpc.org`; the publicnode
  Sepolia endpoint timed out on some `eth_getLogs` calls during dynamic-source
  deploy testing.
- Keep provider wiring out of the core runtime. DigitalOcean is a likely target,
  but the core container should stay portable.
