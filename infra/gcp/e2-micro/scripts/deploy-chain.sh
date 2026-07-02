#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAIN_FILE="${1:-}"

if [ -z "$CHAIN_FILE" ]; then
  echo "usage: $0 <chain-manifest.yaml>" >&2
  exit 2
fi

if [ ! -f "$CHAIN_FILE" ]; then
  echo "chain manifest not found: ${CHAIN_FILE}" >&2
  exit 2
fi

yaml_value() {
  awk -v key="$1" '
    BEGIN { FS = ":" }
    $1 == key {
      sub(/^[^:]*:[[:space:]]*/, "")
      sub(/[[:space:]]+#.*$/, "")
      gsub(/^[[:space:]]+|[[:space:]]+$/, "")
      gsub(/^"|"$/, "")
      print
      exit
    }
  ' "$CHAIN_FILE"
}

export UGRAPH_DEPLOYMENT="${UGRAPH_DEPLOYMENT:-$(yaml_value deployment)}"
export UGRAPH_DOMAIN="${UGRAPH_DOMAIN:-$(yaml_value domain)}"
export DO_DNS_ZONE="${DO_DNS_ZONE:-$(yaml_value dns_zone)}"
export UGRAPH_CHAIN_ID="${UGRAPH_CHAIN_ID:-$(yaml_value chain_id)}"
export UGRAPH_BLOCK_EXPLORER_URL="${UGRAPH_BLOCK_EXPLORER_URL:-$(yaml_value block_explorer_url)}"
export UGRAPH_RPC_URL="${UGRAPH_RPC_URL:-$(yaml_value rpc_url)}"
export UGRAPH_MANIFEST="${UGRAPH_MANIFEST:-$(yaml_value subgraph_manifest)}"
export UGRAPH_FROM_BLOCK="${UGRAPH_FROM_BLOCK:-$(yaml_value from_block)}"
export UGRAPH_TO_BLOCK="${UGRAPH_TO_BLOCK:-$(yaml_value to_block)}"
export UGRAPH_LOG_SOURCE="${UGRAPH_LOG_SOURCE:-$(yaml_value log_source)}"
export UGRAPH_POLL_INTERVAL_MS="${UGRAPH_POLL_INTERVAL_MS:-$(yaml_value poll_interval_ms)}"
export UGRAPH_MAX_BLOCK_RANGE="${UGRAPH_MAX_BLOCK_RANGE:-$(yaml_value max_block_range)}"
export UGRAPH_RPC_MIN_INTERVAL_MS="${UGRAPH_RPC_MIN_INTERVAL_MS:-$(yaml_value rpc_min_interval_ms)}"
export UGRAPH_SYNC_MAX_BLOCKS_PER_PASS="${UGRAPH_SYNC_MAX_BLOCKS_PER_PASS:-$(yaml_value sync_max_blocks_per_pass)}"
export UGRAPH_SYNC_LIMIT="${UGRAPH_SYNC_LIMIT:-$(yaml_value sync_limit)}"

exec "${SCRIPT_DIR}/deploy.sh"
