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

These events include sanitized request headers, truncated request/response body previews, upstream ID, status, duration, and any proxy error string.

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
  host: \"0.0.0.0\"   # use 0.0.0.0 inside containers
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
    url: "redis://redis:6379"       # rediss:// enables TLS
    username: "${REDIS_USERNAME}"   # optional; overrides URL username
    password: "${REDIS_PASSWORD}"   # optional; overrides URL password
    db: 0                           # optional; database index
    keyPrefix: "ebeacon:cache:mainnet:"
    maxRetries: 3
```

For shared state across replicas:

```yaml
state:
  driver: redis
  redis:
    url: "redis://redis:6379"       # rediss:// enables TLS
    username: "${REDIS_USERNAME}"   # optional; overrides URL username
    password: "${REDIS_PASSWORD}"   # optional; overrides URL password
    db: 0                           # optional; database index
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
go run ./scripts/reliability/ -ebeacon http://127.0.0.1:5555/mainnet -duration 10m -report 1m
```

## Cache Encoding Semantics

- Cached responses are transport-encoding agnostic: a cached plain response can be served gzip-compressed, and a cached gzip-origin response can be served plain.
- Cached responses are not representation-agnostic: JSON and `application/octet-stream` remain distinct cache entries.
- When validating cache behavior, compare decoded payloads for gzip/plain checks and raw bytes for octet-stream checks.
