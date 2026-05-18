#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-iconic-elevator-394020}"
ZONE="${ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-ugraph-e2-micro}"
WEB_FIREWALL_RULE="${WEB_FIREWALL_RULE:-ugraph-e2-allow-web}"
SSH_FIREWALL_RULE="${SSH_FIREWALL_RULE:-ugraph-e2-allow-ssh}"
NETWORK_NAME="${NETWORK_NAME:-ugraph-net}"
REGION="${REGION:-${ZONE%-*}}"
SUBNET_NAME="${SUBNET_NAME:-ugraph-subnet-${REGION}}"
GCLOUD="${GCLOUD:-/opt/homebrew/bin/gcloud}"
if [ ! -x "$GCLOUD" ]; then
  GCLOUD="gcloud"
fi

if [ "${UGRAPH_DESTROY_CONFIRM:-}" != "delete" ]; then
  echo "Set UGRAPH_DESTROY_CONFIRM=delete to delete ${VM_NAME}, ${WEB_FIREWALL_RULE}, ${SSH_FIREWALL_RULE}, ${SUBNET_NAME}, and ${NETWORK_NAME}." >&2
  exit 2
fi

"$GCLOUD" compute instances delete "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --quiet
"$GCLOUD" compute firewall-rules delete "$WEB_FIREWALL_RULE" --project "$PROJECT_ID" --quiet || true
"$GCLOUD" compute firewall-rules delete "$SSH_FIREWALL_RULE" --project "$PROJECT_ID" --quiet || true
"$GCLOUD" compute networks subnets delete "$SUBNET_NAME" --project "$PROJECT_ID" --region "$REGION" --quiet || true
"$GCLOUD" compute networks delete "$NETWORK_NAME" --project "$PROJECT_ID" --quiet || true
