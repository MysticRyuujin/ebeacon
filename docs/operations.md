# Operations Guide

This guide focuses on day-2 operations: visibility, reliability tuning, and incident handling.

## Health and Dashboard Endpoints

eBeacon exposes lightweight health endpoints for external load balancers and orchestration systems:

- Global health: `/healthz`
- Per-network health: `/{networkId}/healthz`
- Synthetic Beacon API health: `/{networkId}/eth/v1/node/health`

`/healthz` returns a JSON summary across configured networks. It responds with `200 OK` when at least one network is up or degraded, and `503 Service Unavailable` only when every network is down.

`/{networkId}/healthz` returns per-network counts in the form `{"status":"ok|degraded|down","upstreams":N,"healthy":N,"degraded":N,"down":N}`. It responds with `200 OK` when any upstream is up or degraded, and `503 Service Unavailable` when all upstreams in that network are down.

`/{networkId}/eth/v1/node/health` is answered synthetically from eBeacon's upstream view rather than proxied through to a single beacon node. If a client-route prefix or selector is present, such as `/{networkId}/lighthouse/eth/v1/node/health`, the status reflects only that matching upstream subset.

When `ui.enabled: true`, eBeacon serves:

- Web UI: `/webui/`
- Health summary: `/webui/api/health`
- Upstream status: `/webui/api/upstreams`
- Cache stats: `/webui/api/cache`
- Cache entries: `/webui/api/cache/entries?network=mainnet&limit=100&includeBody=true`
- Cache entry delete: `DELETE /webui/api/cache/entries?network=mainnet&key=mainnet:GET:/eth/v1/node/version`
- Sticky sessions: `/webui/api/sessions`
- Fork view: `/webui/api/forks`

These endpoints help validate canonical-fork alignment and upstream readiness.

The Web UI also includes a cache entry browser with exact-key filtering, on-demand body preview, and per-entry delete actions.

`/webui/api/cache/entries` returns cached response metadata across networks. Optional query parameters:

- `network=<id>` filters to a single network.
- `key=<cache-key>` filters to an exact cache key.
- `limit=<n>` caps the number of returned entries, default `100`, max `500`.
- `includeBody=true` includes cached response bodies as base64.

`DELETE /webui/api/cache/entries` removes a specific cached entry. It requires both `network=<id>` and `key=<cache-key>` and returns JSON describing whether an entry was deleted.

## Metrics

Enable Prometheus metrics:

```yaml
metrics:
  enabled: true
  path: /metrics
```

Then scrape `http://<host>:<port>/metrics`.

> ⚠️ **`/metrics` is not authenticated.** The handler is mounted on the same listener as the proxy and is not gated by `auth.keys`. Anyone who can reach the proxy port can read it. The endpoint does not expose credentials, but it does enumerate upstream IDs, request rates, error rates, and latency percentiles — useful recon for an attacker. Either restrict the listener to a private network / loopback, scrape it from inside a trusted network only, or front it with a firewall or auth-aware reverse proxy. The same applies to `/healthz` and `/ready`.

Cache metrics include:

- `ebeacon_cache_hits_total`
- `ebeacon_cache_misses_total`
- `ebeacon_cache_size`

Score-routing visibility metrics include:

- `ebeacon_upstream_score`
- `ebeacon_upstream_score_error_rate`
- `ebeacon_upstream_score_p90_latency_seconds`
- `ebeacon_upstream_score_head_lag`
- `ebeacon_upstream_score_samples`

Per-path score metrics (labelled by `api_path`) include:

- `ebeacon_upstream_score_by_path`
- `ebeacon_upstream_score_error_rate_by_path`
- `ebeacon_upstream_score_p90_latency_seconds_by_path`
- `ebeacon_upstream_score_samples_by_path`

The `api_path` label is a normalized route template (e.g. `/eth/v1/beacon/headers/{block_id}`) rather than a raw URL, so cardinality stays bounded regardless of request volume. These metrics let you see whether a specific endpoint class (validator duties, state queries, blob sidecars, etc.) is driving latency or errors on a particular upstream before the global score reflects it.

Archive routing metrics (see [Configuration: Archive upstreams](configuration.md#archive-upstreams)):

- `ebeacon_upstream_archive{network,upstream}` — gauge, `1` if the upstream is configured with `archive: true`, `0` otherwise.
- `ebeacon_archive_promotion_total{network,reason}` — counter of requests routed to archive upstreams. `reason` is `proactive` (the target slot/epoch was older than the retention window so archive was used on the first attempt) or `pruning_error` (a pruned upstream returned a 404 and the retry budget was promoted to archive candidates).
- `ebeacon_pruning_error_no_archive_total{network}` — counter of pruning-shaped 404s returned to clients because no upstream in the network is marked `archive: true`. A sustained non-zero rate here is a direct signal that configuring an archive upstream would convert those errors into successful responses.

The bundled Grafana overview dashboard has an **Archive Routing** row covering these, including an overlay of promotions vs no-archive 404s to help decide whether to add an archive upstream to a network.

## Logging

`logLevel` supports `debug`, `info`, `warn`, `error`.

Optional failure debug logging writes structured JSON events to a rotating file:

```yaml
debugLogging:
  enabled: true
  path: "/logs/ebeacon-debug.log"
  maxSizeMB: 100
  maxBackups: 10
  maxBodyBytes: 65536
```

Each event is a JSON object written to the log file (one per line) and includes:

| Field         | Description                                                          |
| ------------- | -------------------------------------------------------------------- |
| `kind`        | Event type, e.g. `proxy_error`, `upstream_error`                     |
| `network`     | Network ID                                                           |
| `api_path`    | Normalized route template (e.g. `/eth/v1/beacon/headers/{block_id}`) |
| `method`      | HTTP method                                                          |
| `client_ip`   | Originating client IP (when available)                               |
| `upstream`    | Upstream ID that handled or failed the request                       |
| `selector`    | Client-route selector, if any                                        |
| `status`      | HTTP status code returned                                            |
| `attempt`     | Retry attempt number                                                 |
| `duration_ms` | Round-trip duration in milliseconds                                  |
| `error`       | Proxy or upstream error string                                       |
| `request`     | Sanitized request headers and truncated body preview                 |
| `response`    | Sanitized response headers and truncated body preview                |

Sensitive headers (`Authorization`, `X-API-Key`, `Cookie`, `X-EBEACON-Secret-Token`) and sensitive query parameters (`token`, `key`, `api_key`, etc.) are redacted before logging. Body previews are capped at `maxBodyBytes` and gzip-decoded where possible.

If you run eBeacon in Docker, mount a writable directory or volume at `/logs`. The image runs as a non-root user, so the mounted directory must be writable by the container user.

For troubleshooting:

- use `debug` in staging for route/failsafe behavior visibility
- use `info` in production by default
- increase to `warn`/`error` only when you need quieter logs

## Profiling

Enable the pprof debug server in config:

```yaml
pprof:
  enabled: true
  host: \"0.0.0.0\" # use 0.0.0.0 inside containers
  port: 6060
```

Then collect profiles with standard Go tooling:

```bash
go tool pprof http://localhost:6060/debug/pprof/heap
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
curl http://localhost:6060/debug/pprof/goroutine?debug=2
```

The reliability test script (`scripts/reliability/`) can capture periodic pprof snapshots automatically.

## Reliability Tuning

- **Retry:** use small delay + bounded `maxDelay`; avoid large `maxAttempts`.
- **Hedge:** use only on latency-sensitive read endpoints.
- **Circuit breaker:** start near defaults (`5/2/30s`) and tune per upstream quality.
- **Consensus mode:** reserve for safety-critical reads where stronger agreement matters more than latency.
- **Sticky sessions:** keep enabled for cache locality and stable client behavior.
- **Score routing:** tune `scoreWeights` to match your priorities (latency vs correctness vs head freshness), but remember that `priority` tiers still dominate candidate ordering.
- **Traffic shaping:** use `priority` for hard primary/backup tiers, then use `weight` inside a tier to bias traffic share.

## Primary / Backup Provider Pattern

If you want local or self-owned nodes to take normal traffic and a public or paid provider to act only as fallback:

```yaml
networks:
  - id: mainnet
    upstreams:
      - id: local-a
        url: "http://10.0.0.11:5052"
        priority: 0
      - id: local-b
        url: "http://10.0.0.12:5052"
        priority: 0
        weight: 3
      - id: paid-backup
        url: "https://provider.example.com"
        priority: 1
        weight: 1
    routing:
      loadBalancing: score
    failsafe:
      retry:
        maxAttempts: 2
```

Operationally, this means:

- primary traffic stays on the `priority: 0` tier,
- the backup tier is used when the preferred tier has no eligible upstreams,
- weight only matters among upstreams that share the same priority tier,
- retries can walk into the backup tier when `maxAttempts` is high enough,
- hedging can also send traffic to backup tiers if they are inside the top `maxCount + 1` ordered candidates.

If you want the backup provider to be true cold standby, keep `retry.maxAttempts: 1` and disable hedging for that network.

## Archive Upstream Pattern

If your local nodes are pruned (the CL client default) and clients request data older than those nodes retain — historical blocks, old blob sidecars, pre-finalization state — configure a provider that retains full history and mark it `archive: true`:

```yaml
networks:
  - id: mainnet
    upstreams:
      - id: lighthouse
        url: "http://lh:5052"
        priority: 0
      - id: archive-provider
        url: "https://archive.example-provider.com"
        priority: 10
        archive: true
        headers:
          Authorization: "Bearer ${ARCHIVE_TOKEN}"
        rateLimiting:
          autoTune: true
          initialRate: 10
          maxRate: 25
```

Operationally, this means:

- Recent requests continue to flow to the pruned tier, exactly as before.
- Requests demonstrably targeting historical data (e.g. `/eth/v1/beacon/blob_sidecars/{slot}` where the slot is more than 18 days behind head) are routed to the archive tier on the first attempt, skipping pruned upstreams entirely.
- Requests that can't be classified up front (typically by-root lookups) flow to the pruned tier first; if a 404 comes back, the retry budget is promoted to archive upstreams and the request continues. The client sees success as long as the archive tier can serve it.
- Priority ordering is preserved inside the archive subset, so a local archive node at `priority: 0` would still beat the cloud archive at `priority: 10` if both were archive-capable.

If `ebeacon_pruning_error_no_archive_total{network="mainnet"}` is non-zero without an archive upstream configured, clients are asking for data your pruned nodes cannot serve. Watch `ebeacon_archive_promotion_total` after enabling archive to see the load pattern: a high `reason="proactive"` rate indicates well-classified historical traffic, while a high `reason="pruning_error"` rate indicates root-based lookups or requests near the retention boundary — both are expected and harmless.

If `failsafe.hedge` is enabled for a network that also has archive upstreams, historical requests trigger parallel archive calls under hedge semantics. For metered providers, prefer disabling hedge on the archive-using network, or rely on per-upstream `rateLimiting.autoTune` to throttle to your plan limits.

## Horizontal Scaling

Two independent Redis integrations support horizontal scaling:

- **Shared cache** (`cache.driver: redis`) — response cache shared across all replicas.
- **Shared state** (`state.driver: redis`) — propagates the canonical head block and finalized epoch across replicas so every instance makes consistent routing decisions without waiting for its own health-check cycle.

For shared cache across replicas:

```yaml
cache:
  enabled: true
  driver: redis
  redis:
    url: "redis://redis:6379" # rediss:// enables TLS
    username: "${REDIS_USERNAME}" # optional; overrides URL username
    password: "${REDIS_PASSWORD}" # optional; overrides URL password
    db: 0 # optional; database index
    keyPrefix: "ebeacon:cache:mainnet:"
    maxRetries: 3
```

For shared state across replicas:

```yaml
state:
  driver: redis
  redis:
    url: "redis://redis:6379" # rediss:// enables TLS
    username: "${REDIS_USERNAME}" # optional; overrides URL username
    password: "${REDIS_PASSWORD}" # optional; overrides URL password
    db: 0 # optional; database index
    maxRetries: 3
```

Credentials can also be embedded directly in the URL (`redis://user:pass@host:port/db`), but separate fields are recommended so secrets can be injected via environment variables.

## Common Runbook Checks

1. Confirm upstream health and canonical fork alignment in dashboard APIs.
2. Verify `403` behavior for intended blocked debug/pool paths.
3. Check `cache hit ratio` trend and adjust policies if misses dominate stable endpoints.
4. Inspect `429` rates per upstream; tune upstream auto-rate limits as needed.
5. Validate graceful shutdown behavior during deploys (`SIGTERM`).

## Validation Commands

Use the Go test suite for package-level verification:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Use the runtime harnesses against a deployed instance:

```bash
go run ./scripts/loadtest/ -base http://127.0.0.1:5555/mainnet -concurrency 50 -duration 60

# Basic reliability test
go run ./scripts/reliability/ -ebeacon http://127.0.0.1:5555/mainnet -duration 10m -report 1m

# With API key auth (also accepted via EBEACON_API_KEY env var)
EBEACON_API_KEY=<key> go run ./scripts/reliability/ \
  -ebeacon http://127.0.0.1:5555/mainnet \
  -upstream http://beacon-node:5052 \
  -duration 30m -report 1m
```

The reliability script validates three properties continuously:

1. **Correctness** — eBeacon responses for immutable data (genesis, config) are byte-identical to direct upstream responses.
2. **Cache accuracy** — when eBeacon returns a cache HIT for a specific slot, the body matches what the upstream returns for that same slot.
3. **SSE health** — long-running event-stream connections receive head/finality events at expected intervals with monotonically increasing slot numbers.

Auth flags apply to all requests including the SSE connection:

| Flag       | Env var           | Description                                 |
| ---------- | ----------------- | ------------------------------------------- |
| `-api-key` | `EBEACON_API_KEY` | Sets `X-API-Key` header                     |
| `-auth`    | `EBEACON_AUTH`    | Sets `Authorization: Bearer <token>` header |

## Cache Encoding Semantics

- Cached responses are transport-encoding agnostic: a cached plain response can be served gzip-compressed, and a cached gzip-origin response can be served plain.
- Cached responses are not representation-agnostic: JSON and `application/octet-stream` remain distinct cache entries.
- When validating cache behavior, compare decoded payloads for gzip/plain checks and raw bytes for octet-stream checks.
