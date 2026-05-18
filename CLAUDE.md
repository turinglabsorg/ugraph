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
  subscriptions, run a bounded `chain-reader` pass, and sync a deployment from
  `UGRAPH_LOG_SOURCE=postgres-feed`.
- Implemented feed schema tables: `ugraph_feed_subscriptions`,
  `ugraph_raw_blocks`, and `ugraph_raw_logs`.
- Docker supports `UGRAPH_MODE=serve|indexer|chain-reader`. The entrypoint also
  forwards normal `ugraph` subcommands such as `deploy`, `chain-reader`, and
  `--help`.
- GraphiQL is served from pinned React/GraphiQL assets and includes a built-in
  fallback query UI if external assets fail.
- Keep provider wiring out of the core runtime. DigitalOcean is a likely target,
  but the core container should stay portable.
