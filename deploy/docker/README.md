# Docker Compose deployment

This deployment packages the existing same-process relay/history design. It improves image,
permission, restart, and rollout control; it does not turn history into a worker process or provide
stronger market-latency isolation.

The supported layout uses Linux host networking and local bind mounts:

- `relay` is the long-running service and mounts the actively filled cache root read-only, observing each
  atomically committed coverage update without publication or restart;
- `publisher` is an optional legacy/rollback profile and mounts both writable and published roots read/write.

Do not use NFS, an object-store mount, or a remote volume driver. Snapshot publication requires the
documented local-filesystem rename, fsync, advisory-lock, and same-filesystem semantics.

## Prepare host paths and configuration

Set `TQSDK_LIVE_CACHE_ROOT` to the cache root used by `tqsdk-cache fill`. Create the configured paths
before starting Compose. Their owner must match `TQSDK_CONTAINER_UID` and
`TQSDK_CONTAINER_GID`; do not recursively change ownership of an existing cache without checking its
current writer.

```bash
sudo install -d -o 10001 -g 10001 -m 0750 \
  /var/lib/tqsdk/history-writable \
  /var/lib/tqsdk/history-published

cp deploy/docker/.env.example deploy/docker/.env
cp deploy/docker/relay.env.example deploy/docker/relay.env
chmod 0600 deploy/docker/.env deploy/docker/relay.env
```

Keep both listeners on `127.0.0.1` with host networking. A controlled local gateway must strip any
client-supplied trusted identity header, inject `TQSDK_RELAY_HISTORY_IDENTITY_HEADER`, and enforce the
deployment's low-concurrency quota.

## Build images

```bash
docker compose \
  --env-file deploy/docker/.env \
  -f deploy/docker/compose.yaml \
  build relay publisher
```

The Dockerfile has separate `relay` and `cache` runtime targets. Both run as a non-root numeric user;
the relay image does not contain the publisher binary.

The image builds the dashboard with the same Node 24 / pnpm 10 major versions as CI, runs its size
check, and embeds the generated assets in the relay binary. The runtime includes timezone data and
defaults to `Asia/Shanghai`; set `TQSDK_TIMEZONE` if the deployment's documented local 08:30 refresh
belongs to another timezone.

The build context is allowlisted to workspace manifests and `crates/**`. This repository does not
track the workspace `Cargo.lock`; a clean image build generates one and then uses `--locked` for the
remaining build. Deploy the resulting immutable image digest rather than assuming a later rebuild of
the same source resolves identical dependency versions.

## Optional published-snapshot compatibility path

The default relay does not require this section. Use it only when operating the retained
`TQSDK_RELAY_HISTORY_ROOT` compatibility adapter or testing rollback artifacts.

Start with the read-only clone plan:

```bash
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml \
  --profile publisher run --rm publisher \
  --output-format json snapshot \
  --history-root /var/lib/tqsdk/history-published dry-run \
  --source-cache-dir /var/lib/tqsdk/history-writable
```

Create a staging generation. Repeat `--catalog-symbol` for the complete served universe; omit
`--catalog-complete` unless that list is authoritative.

```bash
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml \
  --profile publisher run --rm publisher \
  --output-format json snapshot \
  --history-root /var/lib/tqsdk/history-published clone \
  --source-cache-dir /var/lib/tqsdk/history-writable \
  --catalog-complete --catalog-symbol SHFE.au2612
```

Copy the returned snapshot ID. Verification requests must cover each actual data role in the
generation. For example, save this as the gitignored local file `deploy/docker/verify.json`:

```json
{
  "requests": [
    {
      "series": "tick",
      "request_id": 1,
      "symbol": "SHFE.au2612",
      "start_ns": 1787932800000000000,
      "end_ns": 1788019200000000000
    }
  ]
}
```

Verify and publish, substituting the real ID:

```bash
TQ_SNAPSHOT_ID=s-20260829-8d19c4af

docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml \
  --profile publisher run --rm \
  -v "$PWD/deploy/docker/verify.json:/verify/requests.json:ro" publisher \
  snapshot --history-root /var/lib/tqsdk/history-published verify \
  --snapshot-id "$TQ_SNAPSHOT_ID" --request-file /verify/requests.json

docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml \
  --profile publisher run --rm publisher \
  snapshot --history-root /var/lib/tqsdk/history-published publish \
  --snapshot-id "$TQ_SNAPSHOT_ID"
```

If `prewarm` is used, it may return a different snapshot ID. Verify and publish that returned ID.
Remote `fill` is the only part of this workflow that may require `TQ_AUTH_USER` and `TQ_AUTH_PASS`;
pass them from a protected environment rather than adding them to Compose or the repository.

## Start and verify relay

```bash
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml up -d relay
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml ps
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml logs --tail=100 relay

curl --fail-with-body --silent --show-error http://127.0.0.1:7789/health | jq
curl --fail-with-body --silent --show-error http://127.0.0.1:7789/health \
  | jq --exit-status '.history.ready == true'
curl --fail-with-body --silent --show-error http://127.0.0.1:7789/metrics | jq '.history'
curl --fail-with-body --silent --show-error \
  -H 'X-Trusted-Identity: local-ops' \
  http://127.0.0.1:7790/v1/history/schema | jq
```

The Compose healthcheck only proves metrics-listener liveness. The second health command above is the
external history-readiness gate. In default live-cache mode, a missing operation lock or unreadable
cache root leaves market alive while history query and coverage return `503 history_unavailable`.

## Operations and limits

- Build and deploy an immutable image tag or digest before production; do not rely on `:local`.
- Use `docker compose run --rm publisher ...` for recover, rollback, scrub, and GC. Mutating variants
  retain their existing explicit `--apply` gates.
- Do not add a `512m` container limit: 512 MiB is the history buffer budget, not total relay RSS.
  Measure steady-state and peak RSS before setting a container memory limit.
- Docker CPU limits do not isolate SMT siblings, LLC, memory bandwidth, IRQ, or softirq. Keep affinity
  disabled until a separately measured topology plan exists.
- Host networking avoids bridge NAT but shares the host network namespace. Keep ports loopback-only and
  do not publish them with `ports:`.
- The container stop signal is SIGINT so the current Ctrl-C path stops listeners and workers. Active
  requests are abortable during shutdown; the 30-second grace period is not a connection-draining guarantee.
