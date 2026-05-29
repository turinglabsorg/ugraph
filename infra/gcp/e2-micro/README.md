# Google Compute Engine e2-micro

This target runs `ugraph` on the cheapest Google Cloud shape that can host the
runtime continuously: one Compute Engine `e2-micro` VM in an Always Free
eligible US region.

It intentionally avoids Cloud Run, Cloud SQL, and Artifact Registry for the
first deployment path:

- the Docker image is built locally for `linux/amd64`;
- the image archive is copied directly to the VM;
- Postgres runs locally in Docker on the VM's standard persistent disk;
- Caddy terminates HTTPS in front of the API;
- Docker restart policies bring services back after VM reboot.
- the VM boot script enables a 2 GiB swapfile and unattended security updates.
- the VM runs in a dedicated custom VPC instead of the GCP `default` network.

Default profile:

- `postgres`
- `indexer` using `UGRAPH_LOG_SOURCE=rpc`
- `api`
- `caddy`

The shared raw feed is still available for larger machines by setting:

```bash
COMPOSE_PROFILES=feed
UGRAPH_LOG_SOURCE=postgres-feed
```

On `e2-micro`, keep the default compact profile unless you are explicitly
stress-testing the feed worker.

## Deploy

```bash
cd ugraph
PROJECT_ID=iconic-elevator-394020 \
ZONE=us-central1-a \
infra/gcp/e2-micro/scripts/deploy-chain.sh infra/gcp/e2-micro/chains/sepolia.yaml
```

Chain-specific values live in `chains/*.yaml`, not in `.env`. If `rpc_url` is
empty, `ugraph` resolves RPC endpoints from Chainlist. For production indexing,
prefer an explicit RPC URL to avoid public endpoint rate limits.

The script creates or reuses:

- Compute Engine API enablement;
- VPC `ugraph-net` and a regional `/24` subnet;
- firewall rule for public ports `80` and `443`;
- firewall rule for SSH restricted to the deploy operator IP;
- VM `ugraph-e2-micro`;
- `/opt/ugraph/docker-compose.yml`;
- `/opt/ugraph/Caddyfile`;
- `/opt/ugraph/.env`;
- local Docker image loaded into the VM.

Use `UGRAPH_DOMAIN=ugraph.growfi.dev` when the `growfi.dev` zone is available
in DigitalOcean DNS. Set `DO_DNS_ZONE=growfi.dev` to let the deploy script
create or update the matching `A` record automatically. If `UGRAPH_DOMAIN` is
omitted, the deploy script uses `<external-ip>.sslip.io`. That gives the VM a
DNS hostname without buying a domain, and Caddy can issue a normal TLS
certificate for it.

Security defaults:

- only ports `80` and `443` are opened publicly;
- SSH is restricted to the deploy operator IP, not `0.0.0.0/0`;
- the API listens only inside the Docker network;
- Postgres has no published host port;
- runtime secrets live in `/opt/ugraph/.env` with `0600` permissions;
- Caddy adds baseline security headers and redirects HTTP to HTTPS.

`UGRAPH_POLL_INTERVAL_MS` is deliberately configurable. Use a chain-specific
value: Sepolia can run at `12000`, while faster chains can use lower values
such as `1000`. Chain timing belongs in deployment/chain manifests, not in
secrets.

## Status

```bash
PROJECT_ID=iconic-elevator-394020 \
ZONE=us-central1-a \
infra/gcp/e2-micro/scripts/status.sh
```

The public API is served on:

```text
https://<domain>/graphql
https://<domain>/status
https://<domain>/healthz
```

Authenticated remote deploys are served by the same API process:

```bash
ugraph auth login \
  --endpoint https://<domain> \
  --api-key <ugraph-api-key>

ugraph deploy --provider remote \
  --deployment growfi \
  --version 4.0.4 \
  --visibility public \
  --chain-id 11155111 \
  --manifest path/to/subgraph.yaml \
  --rpc-url <rpc>
```

The uploaded bundle is persisted in the `ugraph-data` Docker volume under
`/data/remote-deployments`. Remote deploys run a bounded server-side sync and
promote the requested version to the `latest` query alias when sync succeeds.
The deploy script creates or reuses `UGRAPH_BOOTSTRAP_API_KEY` in
`/opt/ugraph/.env` and prints the matching `ugraph auth login ...` command at
the end of each deploy.

## Users and API keys

The e2-micro target does not expose Postgres publicly. Run control-plane CLI
commands on the VM, over an SSH tunnel, or from a trusted host that can reach
the database. The schema supports:

- users keyed by normalized email;
- API keys stored as hashes only;
- `public_user_signup` enabled/disabled state;
- deployment metadata with version label, owner, API-key prefix, and
  `public|private` visibility.

Private deployments require `Authorization: Bearer <key>` or `x-api-key` with
`query` scope on `/graphql`. Deployments without metadata stay public during
upgrades.

## Destroy

```bash
UGRAPH_DESTROY_CONFIRM=delete \
PROJECT_ID=iconic-elevator-394020 \
ZONE=us-central1-a \
infra/gcp/e2-micro/scripts/destroy.sh
```

Deleting the VM also deletes its boot disk and local Postgres data.
