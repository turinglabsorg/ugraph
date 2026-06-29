#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${DEPLOY_DIR}/../../.." && pwd)"

PROJECT_ID="${PROJECT_ID:-iconic-elevator-394020}"
ZONE="${ZONE:-us-central1-a}"
REGION="${REGION:-${ZONE%-*}}"
VM_NAME="${VM_NAME:-ugraph-e2-micro}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-medium}"
DISK_SIZE_GB="${DISK_SIZE_GB:-30}"
NETWORK_NAME="${NETWORK_NAME:-ugraph-net}"
SUBNET_NAME="${SUBNET_NAME:-ugraph-subnet-${REGION}}"
SUBNET_RANGE="${SUBNET_RANGE:-10.42.0.0/24}"
WEB_TAG="${WEB_TAG:-ugraph-web}"
SSH_TAG="${SSH_TAG:-ugraph-ssh}"
WEB_FIREWALL_RULE="${WEB_FIREWALL_RULE:-ugraph-e2-allow-web}"
SSH_FIREWALL_RULE="${SSH_FIREWALL_RULE:-ugraph-e2-allow-ssh}"
IMAGE_TAG="${IMAGE_TAG:-$(git -C "${REPO_ROOT}" rev-parse --short HEAD)}"
UGRAPH_IMAGE="${UGRAPH_IMAGE:-ugraph-core:${IMAGE_TAG}}"
REMOTE_DIR="${REMOTE_DIR:-/opt/ugraph}"
UGRAPH_POSTGRES_DB="${UGRAPH_POSTGRES_DB:-ugraph}"
UGRAPH_POSTGRES_USER="${UGRAPH_POSTGRES_USER:-ugraph}"
generate_password() {
  od -An -N24 -tx1 /dev/urandom | tr -d ' \n'
}

UGRAPH_POSTGRES_PASSWORD="${UGRAPH_POSTGRES_PASSWORD:-}"
UGRAPH_BOOTSTRAP_API_KEY="${UGRAPH_BOOTSTRAP_API_KEY:-}"
UGRAPH_DEPLOY_AUTH_MODE="${UGRAPH_DEPLOY_AUTH_MODE:-owner}"
UGRAPH_DEPLOYMENT="${UGRAPH_DEPLOYMENT:-growfi}"
UGRAPH_MANIFEST="${UGRAPH_MANIFEST:-/app/examples/growfi/subgraph.yaml}"
UGRAPH_CHAIN_ID="${UGRAPH_CHAIN_ID:-11155111}"
UGRAPH_BLOCK_EXPLORER_URL="${UGRAPH_BLOCK_EXPLORER_URL:-}"
UGRAPH_RPC_URL="${UGRAPH_RPC_URL:-}"
UGRAPH_FROM_BLOCK="${UGRAPH_FROM_BLOCK:-10845295}"
UGRAPH_TO_BLOCK="${UGRAPH_TO_BLOCK:-}"
UGRAPH_LOG_SOURCE="${UGRAPH_LOG_SOURCE:-rpc}"
COMPOSE_PROFILES="${COMPOSE_PROFILES:-}"
DO_DNS_ZONE="${DO_DNS_ZONE:-}"
DO_DNS_TTL="${DO_DNS_TTL:-30}"
DOCTL="${DOCTL:-/opt/homebrew/bin/doctl}"
if [ ! -x "$DOCTL" ]; then
  DOCTL="doctl"
fi

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require docker
require git
require gzip
require mktemp
GCLOUD="${GCLOUD:-/opt/homebrew/bin/gcloud}"
if [ ! -x "$GCLOUD" ]; then
  GCLOUD="gcloud"
fi
require "$GCLOUD"
if [ -n "$DO_DNS_ZONE" ]; then
  require "$DOCTL"
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "project=${PROJECT_ID}"
echo "zone=${ZONE}"
echo "network=${NETWORK_NAME}"
echo "vm=${VM_NAME}"
echo "image=${UGRAPH_IMAGE}"

"$GCLOUD" services enable compute.googleapis.com --project "$PROJECT_ID"

if ! "$GCLOUD" compute networks describe "$NETWORK_NAME" --project "$PROJECT_ID" >/dev/null 2>&1; then
  "$GCLOUD" compute networks create "$NETWORK_NAME" \
    --project "$PROJECT_ID" \
    --subnet-mode custom
fi

if ! "$GCLOUD" compute networks subnets describe "$SUBNET_NAME" --project "$PROJECT_ID" --region "$REGION" >/dev/null 2>&1; then
  "$GCLOUD" compute networks subnets create "$SUBNET_NAME" \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    --network "$NETWORK_NAME" \
    --range "$SUBNET_RANGE"
fi

if ! "$GCLOUD" compute firewall-rules describe "$WEB_FIREWALL_RULE" --project "$PROJECT_ID" >/dev/null 2>&1; then
  "$GCLOUD" compute firewall-rules create "$WEB_FIREWALL_RULE" \
    --project "$PROJECT_ID" \
    --network "$NETWORK_NAME" \
    --allow "tcp:80,tcp:443" \
    --target-tags "$WEB_TAG" \
    --description "Allow ugraph HTTPS API"
fi

SSH_SOURCE_RANGE="${SSH_SOURCE_RANGE:-$(curl -fsS https://api.ipify.org)/32}"
if ! "$GCLOUD" compute firewall-rules describe "$SSH_FIREWALL_RULE" --project "$PROJECT_ID" >/dev/null 2>&1; then
  "$GCLOUD" compute firewall-rules create "$SSH_FIREWALL_RULE" \
    --project "$PROJECT_ID" \
    --network "$NETWORK_NAME" \
    --allow "tcp:22" \
    --source-ranges "$SSH_SOURCE_RANGE" \
    --target-tags "$SSH_TAG" \
    --description "Allow operator SSH for ugraph deploy"
else
  "$GCLOUD" compute firewall-rules update "$SSH_FIREWALL_RULE" \
    --project "$PROJECT_ID" \
    --source-ranges "$SSH_SOURCE_RANGE" \
    --target-tags "$SSH_TAG"
fi

if ! "$GCLOUD" compute instances describe "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" >/dev/null 2>&1; then
  "$GCLOUD" compute instances create "$VM_NAME" \
    --project "$PROJECT_ID" \
    --zone "$ZONE" \
    --machine-type "$MACHINE_TYPE" \
    --image-family debian-12 \
    --image-project debian-cloud \
    --boot-disk-size "${DISK_SIZE_GB}GB" \
    --boot-disk-type pd-standard \
    --network "$NETWORK_NAME" \
    --subnet "$SUBNET_NAME" \
    --tags "${WEB_TAG},${SSH_TAG}" \
    --metadata-from-file startup-script="${SCRIPT_DIR}/startup.sh"
fi

CURRENT_TAGS="$("$GCLOUD" compute instances describe "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --format='get(tags.items)' || true)"
for tag in "$WEB_TAG" "$SSH_TAG"; do
  case " ${CURRENT_TAGS} " in
    *" ${tag} "*) ;;
    *) "$GCLOUD" compute instances add-tags "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --tags "$tag" ;;
  esac
done

EXTERNAL_IP="$("$GCLOUD" compute instances describe "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
UGRAPH_DOMAIN="${UGRAPH_DOMAIN:-${EXTERNAL_IP}.sslip.io}"
if [ -z "$UGRAPH_POSTGRES_PASSWORD" ]; then
  existing_password="$(
    "$GCLOUD" compute ssh "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" \
      --command "test -f '${REMOTE_DIR}/.env' && grep '^UGRAPH_POSTGRES_PASSWORD=' '${REMOTE_DIR}/.env' | sed 's/^UGRAPH_POSTGRES_PASSWORD=//'" \
      2>/dev/null || true
  )"
  if [ -n "$existing_password" ]; then
    UGRAPH_POSTGRES_PASSWORD="$existing_password"
  else
    UGRAPH_POSTGRES_PASSWORD="$(generate_password)"
  fi
fi
if [ -z "$UGRAPH_BOOTSTRAP_API_KEY" ]; then
  existing_key="$(
    "$GCLOUD" compute ssh "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" \
      --command "test -f '${REMOTE_DIR}/.env' && grep '^UGRAPH_BOOTSTRAP_API_KEY=' '${REMOTE_DIR}/.env' | sed 's/^UGRAPH_BOOTSTRAP_API_KEY=//'" \
      2>/dev/null || true
  )"
  if [ -n "$existing_key" ]; then
    UGRAPH_BOOTSTRAP_API_KEY="$existing_key"
  else
    UGRAPH_BOOTSTRAP_API_KEY="ugraph_$(generate_password)"
  fi
fi

if [ -n "$DO_DNS_ZONE" ]; then
  case "$UGRAPH_DOMAIN" in
    "$DO_DNS_ZONE") record_name="@";;
    *".${DO_DNS_ZONE}") record_name="${UGRAPH_DOMAIN%.$DO_DNS_ZONE}";;
    *)
      echo "UGRAPH_DOMAIN=${UGRAPH_DOMAIN} is not under DO_DNS_ZONE=${DO_DNS_ZONE}" >&2
      exit 2
      ;;
  esac
  existing_record="$("$DOCTL" compute domain records list "$DO_DNS_ZONE" --format ID,Type,Name --no-header | awk -v name="$record_name" '$2 == "A" && $3 == name {print $1; exit}')"
  if [ -n "$existing_record" ]; then
    "$DOCTL" compute domain records update "$DO_DNS_ZONE" \
      --record-id "$existing_record" \
      --record-type A \
      --record-name "$record_name" \
      --record-data "$EXTERNAL_IP" \
      --record-ttl "$DO_DNS_TTL"
  else
    "$DOCTL" compute domain records create "$DO_DNS_ZONE" \
      --record-type A \
      --record-name "$record_name" \
      --record-data "$EXTERNAL_IP" \
      --record-ttl "$DO_DNS_TTL"
  fi
fi

echo "building linux/amd64 image locally"
docker build --platform linux/amd64 -f "${REPO_ROOT}/core/Dockerfile" -t "$UGRAPH_IMAGE" "${REPO_ROOT}"
docker save "$UGRAPH_IMAGE" | gzip > "${TMP_DIR}/ugraph-image.tar.gz"

cat > "${TMP_DIR}/.env" <<ENV
UGRAPH_IMAGE=${UGRAPH_IMAGE}
UGRAPH_POSTGRES_DB=${UGRAPH_POSTGRES_DB}
UGRAPH_POSTGRES_USER=${UGRAPH_POSTGRES_USER}
UGRAPH_POSTGRES_PASSWORD=${UGRAPH_POSTGRES_PASSWORD}
UGRAPH_BOOTSTRAP_API_KEY=${UGRAPH_BOOTSTRAP_API_KEY}
UGRAPH_DEPLOY_AUTH_MODE=${UGRAPH_DEPLOY_AUTH_MODE}
UGRAPH_DEPLOYMENT=${UGRAPH_DEPLOYMENT}
UGRAPH_MANIFEST=${UGRAPH_MANIFEST}
UGRAPH_CHAIN_ID=${UGRAPH_CHAIN_ID}
UGRAPH_BLOCK_EXPLORER_URL=${UGRAPH_BLOCK_EXPLORER_URL}
UGRAPH_RPC_URL=${UGRAPH_RPC_URL}
UGRAPH_FROM_BLOCK=${UGRAPH_FROM_BLOCK}
UGRAPH_TO_BLOCK=${UGRAPH_TO_BLOCK}
UGRAPH_DOMAIN=${UGRAPH_DOMAIN}
DO_DNS_ZONE=${DO_DNS_ZONE}
DO_DNS_TTL=${DO_DNS_TTL}
NETWORK_NAME=${NETWORK_NAME}
SUBNET_NAME=${SUBNET_NAME}
UGRAPH_LOG_SOURCE=${UGRAPH_LOG_SOURCE}
UGRAPH_POLL_INTERVAL_MS=${UGRAPH_POLL_INTERVAL_MS:-1000}
UGRAPH_RETRY_MAX_MS=${UGRAPH_RETRY_MAX_MS:-60000}
UGRAPH_REORG_POLICY=${UGRAPH_REORG_POLICY:-rollback}
UGRAPH_REORG_CHECK_DEPTH=${UGRAPH_REORG_CHECK_DEPTH:-64}
UGRAPH_HISTORY_LIMIT=${UGRAPH_HISTORY_LIMIT:-256}
UGRAPH_MAX_BLOCK_RANGE=${UGRAPH_MAX_BLOCK_RANGE:-500}
UGRAPH_RPC_RETRIES=${UGRAPH_RPC_RETRIES:-3}
UGRAPH_RPC_TIMEOUT_SECS=${UGRAPH_RPC_TIMEOUT_SECS:-15}
UGRAPH_SYNC_LIMIT=${UGRAPH_SYNC_LIMIT:-500}
UGRAPH_IPFS_GATEWAY=${UGRAPH_IPFS_GATEWAY:-https://ipfs.io/ipfs/}
UGRAPH_IPFS_TIMEOUT_SECS=${UGRAPH_IPFS_TIMEOUT_SECS:-60}
UGRAPH_MAX_IPFS_FILE_BYTES=${UGRAPH_MAX_IPFS_FILE_BYTES:-26214400}
COMPOSE_PROFILES=${COMPOSE_PROFILES}
ENV
cp "${DEPLOY_DIR}/docker-compose.yml" "${TMP_DIR}/docker-compose.yml"
cp "${DEPLOY_DIR}/Caddyfile" "${TMP_DIR}/Caddyfile"

echo "waiting for docker on vm"
for attempt in $(seq 1 60); do
  if "$GCLOUD" compute ssh "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --command "sudo docker --version >/dev/null 2>&1" >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "docker did not become ready on ${VM_NAME}" >&2
    exit 1
  fi
  sleep 10
done

"$GCLOUD" compute ssh "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --command "sudo mkdir -p '${REMOTE_DIR}' && sudo chown \"\$USER\":\"\$USER\" '${REMOTE_DIR}'"
"$GCLOUD" compute scp "${TMP_DIR}/docker-compose.yml" "${TMP_DIR}/Caddyfile" "${TMP_DIR}/.env" "${TMP_DIR}/ugraph-image.tar.gz" "${VM_NAME}:${REMOTE_DIR}/" --project "$PROJECT_ID" --zone "$ZONE"
"$GCLOUD" compute ssh "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --command "chmod 600 '${REMOTE_DIR}/.env'"
"$GCLOUD" compute ssh "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --command "cd '${REMOTE_DIR}' && sudo docker load -i ugraph-image.tar.gz && sudo docker compose up -d && sudo docker compose restart caddy"

echo "ugraph url: https://${UGRAPH_DOMAIN}"
echo "graphql: https://${UGRAPH_DOMAIN}/graphql"
echo "health: https://${UGRAPH_DOMAIN}/healthz"
echo "status: https://${UGRAPH_DOMAIN}/status"
echo "remote auth: ugraph auth login --endpoint https://${UGRAPH_DOMAIN} --api-key ${UGRAPH_BOOTSTRAP_API_KEY}"
