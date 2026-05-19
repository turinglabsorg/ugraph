# CLI Working Notes

- This folder owns the `ugraph` binary and user/operator commands: validation,
  replay, sync, chain-reader, deploy, users, deployments, serve, GraphQL, and
  status pages.
- Runtime/library logic should stay in `core/crates/ugraph-core`,
  `core/crates/ugraph-runtime`, and `core/crates/ugraph-service`. Keep CLI code
  focused on command orchestration and operator UX.
- The core Docker image builds `core/crates/ugraph-node` and must not copy this
  folder.
- The CLI package name and binary name remain `ugraph` even though the crate
  lives outside `core`.
- The status page should show the append-only entity change timeline from
  `ugraph_entity_changes`; retained history is an internal cache for historical
  GraphQL block queries and rollback.
- Run Rust commands from the repository root unless a command explicitly needs a
  different working directory.
