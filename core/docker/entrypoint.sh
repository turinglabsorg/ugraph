#!/usr/bin/env sh
set -eu

mode="${UGRAPH_MODE:-serve}"
manifest="${UGRAPH_MANIFEST:-/app/examples/growfi/subgraph.yaml}"
storage="${UGRAPH_STORAGE:-json}"
state_file="${UGRAPH_STATE_FILE:-/data/state.json}"
deployment="${UGRAPH_DEPLOYMENT:-default}"
host="${UGRAPH_HOST:-0.0.0.0}"
port="${UGRAPH_PORT:-${PORT:-8030}}"
poll_interval_ms="${UGRAPH_POLL_INTERVAL_MS:-1000}"
retry_max_ms="${UGRAPH_RETRY_MAX_MS:-60000}"
reorg_policy="${UGRAPH_REORG_POLICY:-rollback}"
reorg_check_depth="${UGRAPH_REORG_CHECK_DEPTH:-64}"
history_limit="${UGRAPH_HISTORY_LIMIT:-1024}"
max_block_range="${UGRAPH_MAX_BLOCK_RANGE:-2000}"
rpc_retries="${UGRAPH_RPC_RETRIES:-3}"
limit="${UGRAPH_SYNC_LIMIT:-1000}"
log_source="${UGRAPH_LOG_SOURCE:-rpc}"

if [ "$#" -gt 0 ]; then
  case "$1" in
    -h|--help|validate|inspect|rpc|wasm-imports|wasm-exports|handler-exports|abi-events|plan|scan|replay|sync|chain-reader|deploy|users|deployments|serve|compare|conformance|schema|handler-signatures|compat|runtime-check|type-ids|doctor|matrix)
      exec /usr/local/bin/ugraph "$@"
      ;;
    *)
      exec "$@"
      ;;
  esac
fi

case "$mode" in
  chain-reader|reader)
    if [ -z "${UGRAPH_POSTGRES_URL:-}" ]; then
      echo "UGRAPH_POSTGRES_URL is required for UGRAPH_MODE=chain-reader" >&2
      exit 1
    fi
    set -- /usr/local/bin/ugraph chain-reader --manifest "$manifest" \
      --postgres-url "$UGRAPH_POSTGRES_URL" \
      --deployment "$deployment" --watch \
      --poll-interval-ms "$poll_interval_ms" --retry-max-ms "$retry_max_ms" \
      --max-block-range "$max_block_range" --rpc-retries "$rpc_retries"
    if [ -n "${UGRAPH_CHAIN_ID:-}" ]; then
      set -- "$@" --chain-id "$UGRAPH_CHAIN_ID"
    fi
    if [ -n "${UGRAPH_RPC_URL:-}" ]; then
      set -- "$@" --rpc-url "$UGRAPH_RPC_URL"
    fi
    if [ -n "${UGRAPH_FROM_BLOCK:-}" ]; then
      set -- "$@" --from-block "$UGRAPH_FROM_BLOCK"
    fi
    if [ -n "${UGRAPH_TO_BLOCK:-}" ]; then
      set -- "$@" --to-block "$UGRAPH_TO_BLOCK"
    fi
    exec "$@"
    ;;
  serve|api)
    set -- /usr/local/bin/ugraph serve --host "$host" --port "$port"
    if [ "$storage" = "postgres" ]; then
      if [ -z "${UGRAPH_POSTGRES_URL:-}" ]; then
        echo "UGRAPH_POSTGRES_URL is required when UGRAPH_STORAGE=postgres" >&2
        exit 1
      fi
      set -- "$@" --storage postgres --postgres-url "$UGRAPH_POSTGRES_URL" --deployment "$deployment"
    else
      set -- "$@" --storage json --state-file "$state_file"
    fi
    if [ -n "${UGRAPH_CHAIN_ID:-}" ]; then
      set -- "$@" --chain-id "$UGRAPH_CHAIN_ID"
    fi
    if [ -n "${UGRAPH_BLOCK_EXPLORER_URL:-}" ]; then
      set -- "$@" --block-explorer-url "$UGRAPH_BLOCK_EXPLORER_URL"
    fi
    exec "$@"
    ;;
  sync|indexer|worker)
    set -- /usr/local/bin/ugraph sync --manifest "$manifest" --limit "$limit" --watch \
      --poll-interval-ms "$poll_interval_ms" --retry-max-ms "$retry_max_ms" \
      --reorg-policy "$reorg_policy" --reorg-check-depth "$reorg_check_depth" \
      --history-limit "$history_limit" \
      --max-block-range "$max_block_range" --rpc-retries "$rpc_retries" \
      --log-source "$log_source"
    if [ "$storage" = "postgres" ]; then
      if [ -z "${UGRAPH_POSTGRES_URL:-}" ]; then
        echo "UGRAPH_POSTGRES_URL is required when UGRAPH_STORAGE=postgres" >&2
        exit 1
      fi
      set -- "$@" --storage postgres --postgres-url "$UGRAPH_POSTGRES_URL" --deployment "$deployment"
    else
      set -- "$@" --storage json --state-file "$state_file"
    fi
    if [ -n "${UGRAPH_CHAIN_ID:-}" ]; then
      set -- "$@" --chain-id "$UGRAPH_CHAIN_ID"
    fi
    if [ -n "${UGRAPH_RPC_URL:-}" ]; then
      set -- "$@" --rpc-url "$UGRAPH_RPC_URL"
    fi
    if [ -n "${UGRAPH_FROM_BLOCK:-}" ]; then
      set -- "$@" --from-block "$UGRAPH_FROM_BLOCK"
    fi
    if [ -n "${UGRAPH_TO_BLOCK:-}" ]; then
      set -- "$@" --to-block "$UGRAPH_TO_BLOCK"
    fi
    if [ "${UGRAPH_RESET:-false}" = "true" ]; then
      set -- "$@" --reset
    fi
    exec "$@"
    ;;
  *)
    echo "unknown UGRAPH_MODE: $mode" >&2
    exit 64
    ;;
esac
