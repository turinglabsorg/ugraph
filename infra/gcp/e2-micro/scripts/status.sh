#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-iconic-elevator-394020}"
ZONE="${ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-ugraph-e2-micro}"
REMOTE_DIR="${REMOTE_DIR:-/opt/ugraph}"
GCLOUD="${GCLOUD:-/opt/homebrew/bin/gcloud}"
if [ ! -x "$GCLOUD" ]; then
  GCLOUD="gcloud"
fi

"$GCLOUD" compute instances describe "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --format='table(name,status,machineType.basename(),networkInterfaces[0].accessConfigs[0].natIP)'
"$GCLOUD" compute ssh "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --command "cd '${REMOTE_DIR}' && sudo docker compose ps && sudo docker compose exec -T api curl -fsS http://127.0.0.1:8030/healthz"
