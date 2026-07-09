# Configuration Reference

eBeacon is configured with YAML. Use `ebeacon.example.yaml` as a full commented template.

## Top-Level Sections

| Key            | Purpose                                                       |
| -------------- | ------------------------------------------------------------- |
| `logLevel`     | `debug`, `info`, `warn`, `error`                              |
| `debugLogging` | Optional rotating failure log with request/response previews  |
| `server`       | Listener host/port, timeout, gzip behavior                    |
| `pprof`        | Optional Go runtime profiling debug server                    |
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
- `server.maxResponseBodyBytes`: `2147483648` (2 GiB)
- `server.trustedProxies`: unset (forwarding headers trusted from any peer — see [`server`](#server))
- `pprof.enabled`: `false`
- `pprof.host`: `127.0.0.1`
- `pprof.port`: `6060`
- `debugLogging.maxSizeMB`: `100`
- `debugLogging.maxBackups`: `10`
- `debugLogging.maxBodyBytes`: `65536`
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
- `cache.redis.keyPrefix`: `ebeacon:<networkId>:` when `driver: redis` and no prefix is set
- `upstream.weight`: `1` when omitted
- `upstream.archive`: `false` when omitted (default is pruned, matching CL client defaults)

Validation examples:

- At least one network is required.
- Each network must have at least one upstream.
- `server.port` must be `1..65535`.
- `server.trustedProxies` entries must be valid CIDRs or IPs.
- `debugLogging.path` is required when `debugLogging.enabled` is `true`.
- `cors.allowedOrigins` must contain at least one origin when `cors:` is configured.
- `metrics.path` must start with `/`.
- `cache.maxSize` must be `> 0`.
- `cache.driver` must be `memory` or `redis`; `state.driver` must be `local` or `redis` (typos no longer fall back silently).
- `cache.redis.url` is required when `cache.driver: redis` and the cache is enabled.
- `auth.keys[]` entries require a non-empty, unique `id` and a non-empty `secret`; a key's `tier` must name a defined tier. Keys without an `id` previously authenticated but silently bypassed per-key and tier rate limits.
- `ui.basePath` must start with `/` and must not be the root path when the UI is enabled.
- `blockedPaths`, `routeRules.pathPattern`, and `cache.policies.pattern` must be valid regex.

## `server`

```yaml
server:
  host: "0.0.0.0"
  port: 5555
  maxTimeout: 60s
  enableGzip: true
  maxResponseBodyBytes: 2147483648 # 2 GiB
  trustedProxies: ["10.0.0.0/8", "127.0.0.1"]
```

`maxTimeout` drives request timeout policy ceilings and contributes to HTTP server write timeout. It also bounds multiplexed request executions, which run detached from any single client's connection.

`maxResponseBodyBytes` caps how much of an upstream response body (after gzip decompression) eBeacon buffers. Large beacon states can approach 1 GiB as JSON; the cap bounds memory against oversized or decompression-bomb responses from a misbehaving upstream. Over-limit responses surface to the client as `502`.

`trustedProxies` lists CIDRs or IPs whose `X-Forwarded-For` / `X-Real-IP` headers are honored when deriving the client IP for per-IP rate limiting and sticky sessions. When set, the `X-Forwarded-For` chain is walked right-to-left past trusted hops, so a client cannot spoof its own address. **When unset, forwarding headers are trusted from any peer** — fine behind a proxy that overwrites them, but spoofable if eBeacon is exposed directly to clients. See [Operations: Deployment topology](operations.md#deployment-topology).

Request bodies are capped at 32 MiB; larger bodies return `413 Request Entity Too Large`.

## `pprof`

```yaml
pprof:
  enabled: true
  host: "127.0.0.1" # listen address (default 127.0.0.1)
  port: 6060 # listen port (default 6060)
```

When enabled, eBeacon starts a separate HTTP server exposing Go's standard `net/http/pprof` handlers. Access profiles at `http://<host>:<port>/debug/pprof/`.

The default listen address is loopback-only. Use `0.0.0.0` inside containers and expose the port only on a trusted interface.

## `debugLogging`

```yaml
debugLogging:
  enabled: false
  path: "/logs/ebeacon-debug.log"
  maxSizeMB: 100
  maxBackups: 10
  maxBodyBytes: 65536
```

When enabled, eBeacon writes structured JSON log entries for failed proxy exchanges and upstream attempt failures. Each entry includes the network, normalized API path, upstream ID, status code, duration, error string, sanitized request/response headers, and truncated body previews.

- `path` is the on-disk log file path.
- `maxSizeMB` is the rotation threshold for a single file.
- `maxBackups` is the number of rotated files to keep.
- `maxBodyBytes` caps the body preview stored in each log event. Gzip-encoded bodies are decoded before truncation. Set to `0` to suppress body previews entirely.

Sensitive headers (`Authorization`, `X-API-Key`, `Cookie`, `X-EBEACON-Secret-Token`) and sensitive query parameters are automatically redacted before writing.

For container deployments, mount a writable directory at `/logs` or change `path` to another writable location.

## `cors`

```yaml
cors:
  allowedOrigins:
    - "*"
  allowedMethods: [GET, HEAD, POST, OPTIONS]
  allowedHeaders:
    [content-type, authorization, x-ebeacon-secret-token, x-api-key]
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

Network-level failsafe config merges with global values field by field: a network block that sets only `retry.delay` inherits the global `timeout`, `maxAttempts`, and everything else it doesn't mention. `consensus` can also be overridden (or disabled) per network. Upstream-level `failsafe.circuitBreaker` overrides the merged result for that upstream.

## `networks` and upstreams

```yaml
networks:
  - id: mainnet
    genesisTime: 1606824023 # optional; enables slot-boundary TTL alignment
    upstreams:
      - id: lighthouse
        url: "http://lh-mainnet:5052"
        priority: 0
        headers:
          Authorization: "Bearer token"
```

- Lower `priority` is preferred. `0` is your primary tier, `1` is a backup tier, `2` is a deeper fallback tier, and so on.
- `weight` biases routing inside a priority tier. Higher weight means more traffic share for `round-robin`, `random`, and `score`, and more effective capacity for `least-conn`.
- `archive: true` marks the upstream as retaining full chain history. Used only for requests that target data older than a pruned node retains. See [Archive upstreams](#archive-upstreams) below.
- Optional per-upstream `rateLimiting.autoTune` adapts to upstream `429` responses.
- Top-level `auth:` applies here, including path-auth `/{apiKey}/{networkId}/eth/v1/...`.

### `genesisTime` and slot-boundary TTL alignment

When `genesisTime` is known, eBeacon aligns the cache TTL for head-relative requests (`/head`, `/finalized`, `/justified`, and bare `/eth/v1/beacon/headers`) to expire at the next Ethereum slot boundary rather than after a fixed duration.

**Why this matters:** Ethereum produces a block every 12 seconds. A response cached 11 seconds into a slot would otherwise remain fresh for its full configured TTL, serving stale data well into the following slot. With slot alignment, the TTL is capped to the time remaining in the current slot (minimum 1 s), so cached data never crosses a slot boundary.

**Built-in defaults:** eBeacon automatically applies the correct genesis time for the three standard networks — no configuration needed.

| Network | `genesisTime` (default) |
| ------- | ----------------------- |
| mainnet | `1606824023`            |
| sepolia | `1655733600`            |
| hoodi   | `1742213400`            |

For any other network, set `genesisTime` to the unix timestamp of its genesis block. These values are permanent chain constants and never change.

Set `secondsPerSlot` (default `12`) for chains that don't use 12-second slots — e.g. Gnosis/Chiado use `5`. It drives both slot-boundary TTL alignment and the block cache's future-slot plausibility guard; leaving it at the 12s default on a faster chain would cause eBeacon to reject legitimate blocks and silently disable fork detection.

Set `slotsPerEpoch` (default `32`) for chains with a different epoch length — Gnosis/Chiado use `16` and are auto-detected by network `id`. It drives finality-aware cache promotion: with the wrong value, slots past the true finalized checkpoint are treated as immutable and cached forever, so reorgeable data can be served permanently.

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

### Archive upstreams

Consensus-layer (CL) nodes prune historical state by default. Typical client retention is roughly 8 days of historical states, ~18 days of blob sidecars, and several months of blocks. Requests for data older than that return a 404 from a pruned node. Marking specific upstreams as **archive** tells eBeacon they retain full history and can serve those historical-data requests.

```yaml
networks:
  - id: mainnet
    upstreams:
      - id: lighthouse
        url: "http://lh:5052"
        priority: 0
      - id: prysm
        url: "http://prysm:3500"
        priority: 0
      - id: quicknode
        url: "https://your-endpoint.quiknode.pro/..."
        priority: 10
        archive: true
        headers:
          Authorization: "Bearer ${QUICKNODE_TOKEN}"
        rateLimiting:
          autoTune: true
          initialRate: 10
          maxRate: 25
```

The default is `archive: false`, which matches the CL client default — only set `archive: true` on upstreams you have verified retain full history.

Two routing behaviors are driven by the flag:

1. **Proactive routing on first attempt.** When the request path carries a numeric slot, epoch, or block root, eBeacon extracts the target identifier and compares it to a per-endpoint retention window:

   | Endpoint family | Retention threshold | Source |
   | --- | --- | --- |
   | `/eth/v1/beacon/blob_sidecars/{block_id}` | 4096 epochs (~18 days) | `MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS` (EIP-4844) |
   | `/eth/v1/beacon/states/{state_id}/...` | 8192 slots (~27 hours) | conservative; real Lighthouse/Prysm retention varies |
   | `/eth/v{1,2}/beacon/blocks/{block_id}` and related | 33024 epochs (~5 months) | `MIN_EPOCHS_FOR_BLOCK_REQUESTS` (spec) |
   | `/eth/v1/validator/duties/{attester,proposer,sync}/{epoch}` | current epoch + 1 | Beacon API spec |
   | `/eth/v1/beacon/rewards/attestations/{epoch}` | current epoch + 1 | Beacon API spec |

   When the target is demonstrably older than the threshold, eBeacon routes directly to `archive: true` upstreams on the first attempt and skips the pruned tier entirely. Named identifiers (`head`, `finalized`, `justified`, `genesis`) are always served by pruned nodes and are not classified this way.

2. **Error-driven fallthrough on 404.** For cases proactive classification cannot cover (by-root lookups where the slot is unknown, or requests that sat just inside the conservative threshold but outside the client's actual retention), eBeacon watches for pruning-shaped responses. A 404 on any historical-id path triggers promotion: the remaining retry budget is filled with archive-capable candidates and the request continues. The client does not see the 404 unless the archive tier also fails.

Priority continues to apply inside the archive subset. A `priority: 0` local archive node beats a `priority: 10` cloud archive provider. The same `weight` and load-balancing rules apply between equal-priority archive upstreams.

If no upstream in the pool is marked `archive: true`, behavior is unchanged from pre-archive releases: pruning-shaped 404s propagate to clients, and `ebeacon_pruning_error_no_archive_total` (see [Operations Guide](operations.md#metrics)) ticks so you can see the signal that configuring an archive upstream would convert those errors into successful responses.

**Pruning detection is heuristic-based.** eBeacon treats an HTTP 404 on a historical-id path as pruning-shaped. Body substrings are intentionally not matched (Lighthouse, Prysm, Teku, and Nimbus use different error wording that changes between versions). The cost of a false positive is one extra upstream roundtrip; the archive retry will fail identically and return the 404 the client would have seen anyway.

**Hedge and archive quota.** When `failsafe.hedge` is enabled alongside proactive archive routing, multiple parallel requests fire to archive upstreams for a single historical request. For metered providers (QuickNode, Alchemy tiered plans), either disable hedge for that network or rely on per-upstream `rateLimiting.autoTune` to stay inside your allotment.

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

### Path-aware scoring

In addition to the global score, eBeacon maintains per-path score trackers for each upstream. Paths are normalized to stable route templates (e.g. `/eth/v1/beacon/headers/{block_id}`) before recording, so cardinality stays bounded regardless of request volume.

When routing a request, the path-aware score is used in preference to the global score when enough path-specific samples exist. This means an upstream that is slow only on validator-duty lookups can be deprioritized for that endpoint class without affecting its ranking for other requests.

Path-aware metrics are exposed with the `api_path` label — see [Operations: Metrics](operations.md#metrics).

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
- With `driver: redis`, `redis.keyPrefix` defaults to `ebeacon:<networkId>:` so networks sharing a Redis database cannot see each other's entries during scan-based operations (finality promotion, reorg purge, size metrics). Set an explicit prefix to override; keep it unique per network.
