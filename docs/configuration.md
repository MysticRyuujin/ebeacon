# Configuration Reference

eBeacon is configured with YAML. Use `ebeacon.example.yaml` as a full commented template.

## Top-Level Sections

| Key            | Purpose                                                       |
| -------------- | ------------------------------------------------------------- |
| `logLevel`     | `debug`, `info`, `warn`, `error`                              |
| `server`       | Listener host/port, timeout, gzip behavior                    |
| `cors`         | Optional browser CORS policy for proxy and web UI routes      |
| `failsafe`     | Global timeout/retry/hedge/circuit-breaker/consensus defaults |
| `health`       | Polling intervals and degradation thresholds                  |
| `rateLimiting` | Global per-IP and optional global token bucket                |
| `metrics`      | Prometheus endpoint config                                    |
| `state`        | Shared state backend (`local` or `redis`)                     |
| `ui`           | Embedded dashboard enablement and base path                   |
| `networks`     | Network-based routing mode                                    |

Use top-level `networks:` and optionally top-level `auth:`. Let HAProxy rewrite hostnames into `/{networkId}/...`, `/{networkId}/{clientRoute}/...`, or set `X-EBEACON-Use-Upstream`.

`routing.clientRoutes` are hard selectors: when a client-route path matches, eBeacon stays on that configured upstream or client type and returns `503 Service Unavailable` if the selected client is unavailable. Explicit header/query selectors via `X-EBEACON-Use-Upstream` and `?use-upstream=` behave the same way. For standard `/{networkId}/{selector}/eth/v...` Beacon API paths, eBeacon can also infer selectors automatically from detected client types or exact upstream IDs when no explicit `clientRoutes` entry exists.

## Defaults and Validation Highlights

- `server.host`: `0.0.0.0`
- `server.port`: `5555`
- `server.maxTimeout`: `60s`
- `server.enableGzip`: defaults to enabled when omitted
- `cors.allowedOrigins`: `[*]` when a top-level `cors:` block is present
- `cors.allowedMethods`: `GET`, `HEAD`, `POST`, `OPTIONS`
- `cors.allowedHeaders`: `content-type`, `authorization`, `x-ebeacon-secret-token`, `x-api-key`
- `cors.allowCredentials`: `false`
- `cors.maxAge`: `3600`
- `health.checkInterval`: `15s`
- `health.finalityInterval`: `60s`
- `health.maxSyncDistance`: `10`
- `health.followDistance`: `32`
- `health.maxHeadDistance`: `2`
- `metrics.path`: `/metrics`
- `ui.basePath`: `/webui`
- `state.driver`: `local`
- `routing.loadBalancing`: `round-robin`
- `routing.stickyTimeout`: `10m`
- `routing.rebalanceInterval`: `30s`
- `routing.rebalanceThreshold`: `1.5`
- `routing.rebalanceMaxSweep`: `10`
- `routing.scoreWindowSize`: `100`
- `cache.maxSize`: `2048`
- `cache.driver`: `memory`
- `upstream.weight`: `1` when omitted

Validation examples:

- At least one network is required.
- Each network must have at least one upstream.
- `server.port` must be `1..65535`.
- `cors.allowedOrigins` must contain at least one origin when `cors:` is configured.
- `metrics.path` must start with `/`.
- `cache.maxSize` must be `> 0`.
- `blockedPaths`, `routeRules.pathPattern`, and `cache.policies.pattern` must be valid regex.

## `server`

```yaml
server:
  host: "0.0.0.0"
  port: 5555
  maxTimeout: 60s
  enableGzip: true
```

`maxTimeout` drives request timeout policy ceilings and contributes to HTTP server write timeout.

## `cors`

```yaml
cors:
  allowedOrigins:
    - "*"
  allowedMethods: [GET, HEAD, POST, OPTIONS]
  allowedHeaders: [content-type, authorization, x-ebeacon-secret-token, x-api-key]
  exposedHeaders: [X-Ebeacon-Upstream, X-Ebeacon-Client-Type, X-Ebeacon-Cache]
  allowCredentials: false
  maxAge: 3600
```

Notes:

- The `cors:` block is optional. When omitted, eBeacon does not inject CORS headers.
- Preflight `OPTIONS` requests are answered locally and never forwarded upstream.
- Allowed origins are exact matches, except `*` which permits all origins.
- CORS headers are added outside the cache layer, so cached entries do not pin one browser origin.

## `failsafe`

```yaml
failsafe:
  timeout:
    duration: 30s
  retry:
    maxAttempts: 3
    delay: 100ms
    backoff: 2.0
    jitter: 50ms
    maxDelay: 5s
  hedge:
    delay: 500ms
    maxCount: 1
  circuitBreaker:
    failureThreshold: 5
    successThreshold: 2
    halfOpenAfter: 30s
  consensus:
    enabled: false
    maxParticipants: 3
    agreementThreshold: 2
```

Network-level and upstream-level failsafe config overrides global values.

## `networks` and upstreams

```yaml
networks:
  - id: mainnet
    upstreams:
      - id: lighthouse
        url: "http://lh-mainnet:5052"
        priority: 0
        headers:
          Authorization: "Bearer token"
```

- Lower `priority` is preferred. `0` is your primary tier, `1` is a backup tier, `2` is a deeper fallback tier, and so on.
- `weight` biases routing inside a priority tier. Higher weight means more traffic share for `round-robin`, `random`, and `score`, and more effective capacity for `least-conn`.
- Optional per-upstream `rateLimiting.autoTune` adapts to upstream `429` responses.
- Top-level `auth:` applies here, including path-auth `/{apiKey}/{networkId}/eth/v1/...`.

### Preferred local + backup public provider

This is the recommended pattern when you want self-hosted nodes to absorb normal traffic and a third-party provider to act only as fallback:

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
      - id: public-backup
        url: "https://public.example.com"
        priority: 1
        weight: 1
    routing:
      loadBalancing: score
```

With `retry.maxAttempts: 1`, the backup tier is only used when no primary-tier upstream is eligible.

With `retry.maxAttempts > 1`, retries walk the ordered list, so backup tiers can receive traffic after failures from the preferred tier.

## `routing`

```yaml
routing:
  loadBalancing: score
  stickySession: true
  stickyTimeout: 10m
  rebalanceInterval: 30s
  rebalanceThreshold: 1.5
  rebalanceMaxSweep: 10
  scoreWeights:
    errorRate: 4.0
    latency: 8.0
    headLag: 2.0
    syncDistance: 1.0
  blockedPaths:
    - "^/eth/v[0-9]+/debug/.*"
```

Strategies: `round-robin`, `random`, `least-conn`, `score`.

### How score routing works

eBeacon first builds the candidate list in this order:

1. Ready upstreams on the canonical fork.
2. Ready upstreams regardless of canonical-fork status.
3. Healthy upstreams.
4. All upstreams.

It then sorts candidates by `priority` first. Load balancing only decides the order within a single priority tier. That means `priority: 0` always beats `priority: 1`, even if the backup tier has a higher raw score or larger weight.

For `loadBalancing: score`, eBeacon computes:

```text
score = (1 - errorRate) * errorWeight
      + latencyTerm * latencyWeight
      + (1 / (1 + headLag)) * headLagWeight
      + (1 / (1 + syncDistance)) * syncDistanceWeight

latencyTerm = 1                    when no successful requests have been sampled yet
latencyTerm = 1 / (1 + p90Seconds) otherwise
```

Where:

- `errorRate` is the rolling error ratio over the configured `scoreWindowSize`, including both proxied requests and internal background health probes.
- `p90Seconds` is the rolling p90 latency of successful upstream interactions, including both proxied requests and successful internal health probes.
- `headLag` is slots behind the pool's canonical head.
- `syncDistance` is the upstream's reported sync distance.

### Does the highest score get all the traffic?

No. Inside a priority tier, score mode now uses weighted sampling without replacement, where each upstream's selection weight is `rawScore * weight`.

- Normal forwarding uses the first upstream in the ordered list.
- `retry.maxAttempts > 1` allows later candidates to receive retry traffic.
- `hedge.maxCount > 0` allows multiple top-ranked candidates to receive parallel traffic.
- `consensus.enabled: true` intentionally queries multiple upstreams.
- Sticky sessions can keep clients pinned to an upstream that was selected earlier, which can also preserve some traffic on non-top-ranked upstreams until rebalance or health changes.

So score mode is weighted probabilistic selection within a priority tier, not pure deterministic ranking.

### How to reduce traffic to a specific upstream

Today the main controls are:

- Put expensive or public backups in a higher `priority` tier.
- Lower `weight` for providers you want to use less within the same priority tier.
- Keep `retry.maxAttempts` low if you only want backups on true failover.
- Keep `hedge` disabled, or set a small `maxCount`, so eBeacon does not fan out to extra upstreams.
- Use `clientRoutes`, `routeRules`, or `X-EBEACON-Use-Upstream` for endpoints that must hit a specific upstream.
- Use upstream auto-tuning if a provider signals overload via `429` responses.

## `cache`

```yaml
cache:
  enabled: true
  maxSize: 4096
  driver: memory
  policies:
    - pattern: "^/eth/v1/node/version$"
      ttl: 1h
      methods: [GET]
```

Notes:

- `TTL: 0` means cache forever.
- If no `policies` are provided, built-in defaults are used.
- Methods default to `GET` and `HEAD` when omitted.
- `driver: redis` requires `cache.redis.url`.
