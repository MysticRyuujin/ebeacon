# eBeacon

eBeacon is a fault-tolerant, high-performance reverse proxy for Ethereum Beacon API nodes. It provides intelligent load balancing, response caching, canonical fork detection, request multiplexing, and multi-upstream reliability for consensus layer infrastructure.

## Features

- Score-based, round-robin, random, and least-connections load balancing
- Canonical fork detection (majority-vote block tracking, auto-excludes forked nodes)
- Auto-detection of consensus client type (Lighthouse, Prysm, Teku, Nimbus, Lodestar, Grandine, Caplin)
- Response caching with finality-aware TTL promotion (in-memory LRU or Redis)
- Request multiplexing (deduplicates identical concurrent requests)
- Sequential retry with configurable backoff and jitter
- Hedged requests (parallel requests after configurable delay)
- Per-upstream circuit breaker (closed/open/half-open states)
- Consensus verification policy (query N upstreams, require M agreement)
- Per-IP and global rate limiting with token bucket
- Per-upstream rate limit auto-tuner (adapts to 429 responses)
- Sticky sessions with automatic rebalancing
- Upstream priority tiers for preferred and backup upstreams
- Archive upstream routing (serve historical queries pruned nodes can't)
- Per-method/path failsafe overrides
- gzip compression (client-proxy and proxy-upstream)
- Configurable CORS for browser clients and frontend apps
- Dugtrio-style client routing (path prefix to specific upstreams)
- Project routing with API key authentication
- SSE (Server-Sent Events) relay with reconnect and deduplication
- Synthetic health endpoints for load balancers: `/healthz` and `/eth/v1/node/health`
- Ethereum consensus header preservation
- Web UI dashboard (health, upstreams, cache, sessions, fork visualization)
- Prometheus metrics with pre-built Grafana dashboard
- Shared state for horizontal scaling (Redis pub/sub)
- Docker support

## Documentation

For multi-page docs, start in [`docs/README.md`](docs/README.md):

- [`docs/getting-started.md`](docs/getting-started.md)
- [`docs/configuration.md`](docs/configuration.md)
- [`docs/routing-and-auth.md`](docs/routing-and-auth.md)
- [`docs/operations.md`](docs/operations.md)

## Quick Start

Create a minimal `ebeacon.yaml` with three mainnet upstreams:

```yaml
logLevel: info

server:
  host: "0.0.0.0"
  port: 5555
  maxTimeout: 60s

networks:
  - id: mainnet
    upstreams:
      - id: lighthouse
        url: "http://127.0.0.1:5052"
      - id: prysm
        url: "http://127.0.0.1:3500"
      - id: teku
        url: "http://127.0.0.1:5051"
    routing:
      loadBalancing: score
      stickySession: true
    cache:
      enabled: true
      maxSize: 4096
```

Build and run:

```bash
go build -o ebeacon .
./ebeacon -config ebeacon.yaml
```

The proxy listens on the configured `server.port` (default `5555`). Beacon API paths are served under `/{networkId}/eth/v1/...` (or prefix-free when only one network is defined).

For readiness checks, eBeacon also exposes a global `/healthz`, a per-network `/{networkId}/healthz`, and synthetic `/eth/v1/node/health` responses that reflect upstream health instead of proxy process liveness alone.

## Docker

Build locally:

```bash
docker build -t ebeacon .
docker run -p 5555:5555 -v $(pwd)/ebeacon.yaml:/app/ebeacon.yaml ebeacon
```

Or pull a published release image from GHCR (image names must be lowercase):

```bash
docker pull ghcr.io/mysticryuujin/ebeacon:latest
docker run -p 5555:5555 -v $(pwd)/ebeacon.yaml:/app/ebeacon.yaml ghcr.io/mysticryuujin/ebeacon:latest
```

The image exposes port `5555` and expects a config file at `/app/ebeacon.yaml` (override by mounting your own file as shown).

## Developer Checks (Hooks + CI)

Local git hooks are provided in `.githooks/`:

- `pre-commit`: runs `gofmt` on staged `.go` files, re-stages them, tests changed packages, and runs `golangci-lint` if installed locally.
- `pre-push`: verifies repo-wide `gofmt`, runs `go vet`, and runs `go test -race ./...`.

Install hooks:

```bash
./scripts/install-hooks.sh
```

Optional `Makefile` shortcuts:

```bash
make fmt
make lint
make test
make ci-local
```

## Validation Harnesses

The repository includes two long-running validation tools under `scripts/`:

- `go run ./scripts/loadtest/ -base http://127.0.0.1:5555/mainnet -concurrency 50 -duration 60`
  Generates mixed Beacon API traffic, including SSE and both `gzip` and `identity` request paths.
- `go run ./scripts/reliability/ -duration 30m -report 1m`
  Continuously checks immutable responses, cache accuracy, cache encoding compatibility, SSE health, and captures pprof snapshots.

Both tools target a live eBeacon instance. The reliability harness can also compare against direct upstream CL REST APIs via `-upstream`.

CI (`.github/workflows/ci.yml`) enforces:

- `gofmt` verification
- `go mod verify`
- `go vet`
- `golangci-lint`
- `go test -v -race -cover ./...`
- `govulncheck`
- Trivy filesystem + image vulnerability scanning
- Docker image build smoke check
- Optional SBOM generation/upload (`CI_GENERATE_SBOM=true` repository variable)

Dependabot config is included at `.github/dependabot.yml` for weekly updates of:

- Go modules (`gomod`)
- GitHub Actions
- Docker base images

## Configuration

Configuration is YAML. See [`ebeacon.example.yaml`](ebeacon.example.yaml) for a full, commented reference covering top-level `networks`, Redis cache/state, and advanced routing.

**Key sections:**

| Section          | Purpose                                                                                                                                        |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| **server**       | `host`, `port`, `maxTimeout`, `enableGzip` (default: gzip enabled)                                                                             |
| **cors**         | `allowedOrigins`, `allowedMethods`, `allowedHeaders`, `exposedHeaders`, `allowCredentials`, `maxAge` for browser access                        |
| **failsafe**     | `timeout`, `retry`, `hedge`, `circuitBreaker`, `consensus` — global defaults merged per network/upstream                                       |
| **health**       | `checkInterval`, `finalityInterval`, `maxSyncDistance`, `followDistance`, `maxHeadDistance` — sync/finality polling and degradation thresholds |
| **rateLimiting** | `perIP`, `global` — token-bucket `limit` and `burst`                                                                                           |
| **metrics**      | `enabled`, `path` — Prometheus scrape endpoint on the proxy port                                                                               |
| **state**        | `driver`: `local` or `redis` — shared pub/sub for multi-instance deployments                                                                   |
| **ui**           | `enabled`, `basePath` — embedded web UI (default base path `/webui`)                                                                           |
| **networks**     | `id`, `upstreams`, `routing`, `cache`, `failsafeOverrides` — per-chain pools and overrides for a single eBeacon deployment                     |

Example skeleton:

```yaml
logLevel: info

server:
  host: "0.0.0.0"
  port: 5555
  maxTimeout: 60s

cors:
  allowedOrigins: ["*"]

failsafe:
  timeout: { duration: 30s }
  retry:
    maxAttempts: 3
    delay: 100ms
    backoff: 2.0
    jitter: 50ms
  hedge: { delay: 500ms, maxCount: 1 }
  circuitBreaker:
    failureThreshold: 5
    successThreshold: 2
    halfOpenAfter: 30s

health:
  checkInterval: 15s
  finalityInterval: 60s
  maxSyncDistance: 10

rateLimiting:
  perIP: { limit: 100, burst: 1000 }

metrics:
  enabled: true
  path: /metrics

state:
  driver: local

ui:
  enabled: true
  basePath: /webui

networks:
  - id: mainnet
    upstreams:
      - id: lh
        url: "http://localhost:5052"
        priority: 0
    routing:
      loadBalancing: score
      scoreWeights:
        errorRate: 4.0
        latency: 8.0
        headLag: 2.0
        syncDistance: 1.0
    cache:
      enabled: true
      driver: memory
      maxSize: 4096
```

## Load Balancing

eBeacon supports four strategies:

| Strategy        | Behavior                                                                               |
| --------------- | -------------------------------------------------------------------------------------- |
| **round-robin** | Cycles upstreams in order for each new request.                                        |
| **random**      | Chooses a uniform random upstream per request.                                         |
| **least-conn**  | Prefers upstreams with fewer active in-flight connections.                             |
| **score**       | Ranks upstreams by a composite quality score (higher is better) using rolling metrics. |

Before load balancing is applied, eBeacon narrows the candidate set in this order:

1. Ready upstreams on the canonical fork.
2. Ready upstreams regardless of canonical-fork status.
3. Healthy upstreams.
4. All upstreams as a last resort.

Then it sorts that set by `priority` first, and only compares load-balancing strategy inside the same priority tier. Lower `priority` numbers always win over higher numbers.

**Score-based routing** computes this raw score, where higher is better:

```text
score = (1 - errorRate) * errorWeight
      + latencyTerm * latencyWeight
      + (1 / (1 + headLag)) * headLagWeight
      + (1 / (1 + syncDistance)) * syncDistanceWeight

latencyTerm = 1                    when there are no latency samples yet
latencyTerm = 1 / (1 + p90Seconds) otherwise
```

The score inputs are:

- `errorRate`: rolling fraction of recent upstream interactions that failed, including internal health probes.
- `p90Seconds`: rolling 90th percentile latency of successful upstream interactions, including internal health probes.
- `headLag`: how many slots this upstream is behind the pool's current canonical head.
- `syncDistance`: the upstream's reported sync distance.

Within a priority tier, eBeacon now applies `upstream.weight` as an actual routing bias:

- `round-robin`: weighted round-robin for the primary pick.
- `random`: weighted random selection without replacement.
- `least-conn`: lower `activeConnections / weight` wins.
- `score`: weighted sampling without replacement using `rawScore * weight`.

Normal requests still use a single upstream: the first entry in the ordered candidate list. With `loadBalancing: score`, that means primary traffic is influenced by both the raw score and `weight` inside the lowest available `priority` tier. Lower-ranked upstreams still receive traffic when:

- the preferred upstream becomes unready or non-canonical,
- `retry.maxAttempts > 1` and eBeacon walks down the ordered list,
- `hedge.maxCount > 0` and eBeacon fires parallel requests to the top few candidates,
- `consensus` mode queries multiple upstreams.

This means score routing is no longer pure highest-score-wins ranking. It is weighted by `rawScore * weight` within a priority tier, with retries and hedges still able to fan out further down the ordered list.

Use `priority` for hard primary / backup separation, and `weight` for softer traffic shaping inside a single priority tier.

For a common primary / backup layout, put self-hosted nodes in `priority: 0` and paid or public fallback providers in `priority: 1` or higher. Example:

```yaml
networks:
  - id: mainnet
    upstreams:
      - id: local-lighthouse-a
        url: "http://10.0.0.11:5052"
        priority: 0
      - id: local-lighthouse-b
        url: "http://10.0.0.12:5052"
        priority: 0
        weight: 3
      - id: paid-backup
        url: "https://beacon.example-provider.com"
        priority: 1
        weight: 1
    routing:
      loadBalancing: score
    failsafe:
      retry:
        maxAttempts: 3
```

With that setup, eBeacon prefers the local tier for primary traffic and only reaches the backup tier when the local tier is exhausted by health, fork exclusion, or retry / hedge behavior.

The live composite score is exposed in both the embedded dashboard (`/webui`, Upstreams table) and Prometheus. The main metrics are `ebeacon_upstream_score`, `ebeacon_upstream_score_error_rate`, `ebeacon_upstream_score_p90_latency_seconds`, and `ebeacon_upstream_score_head_lag`, and the bundled Grafana overview dashboard includes them in the per-upstream table.

## Archive vs Pruned Upstream Routing

Beacon nodes prune historical state by default (about 8 days of full historical states and roughly 18 days of blob sidecars for typical clients — blocks are retained longer but still finite). If every upstream in a pool is pruned, a client asking for `/eth/v2/beacon/blocks/2000000` or a 6-month-old blob sidecar gets a 404 and nothing else eBeacon can do. Marking specific upstreams as **archive** lets eBeacon route those historical-data requests to upstreams that can actually serve them.

```yaml
networks:
  - id: mainnet
    upstreams:
      - id: lighthouse
        url: "http://lh:5052"
        priority: 0 # local pruned
      - id: quicknode
        url: "https://..."
        priority: 10
        archive: true # serves historical data when the local tier can't
```

The default is `archive: false`, matching the CL default. Only mark upstreams you have verified to retain full history.

Two mechanisms decide when archive upstreams get used:

1. **Proactive classification.** Request paths that carry a numeric slot, epoch, or block root are classified up front. If the target is older than the per-endpoint retention window (blob sidecars ~18 days, blocks ~5 months, states ~27 hours, duties 1 epoch), eBeacon skips pruned upstreams on the first attempt and routes to archive-capable ones directly. Named identifiers (`head`, `finalized`, `justified`, `genesis`) and root-based lookups cannot be classified this way and flow through normal routing.

2. **Error-driven fallthrough.** If a pruned upstream returns a 404 for any historical-id path — by-root lookups, or requests that sat just inside our conservative retention threshold but outside the client's actual retention — eBeacon promotes the remaining retry budget to archive upstreams and continues the request without the client seeing the 404.

Priority still applies within the archive subset. A `priority: 0` local archive beats a `priority: 10` cloud archive. If no upstream is marked `archive: true`, behavior matches pre-archive releases: pruning-shaped 404s propagate to the client unchanged, and `ebeacon_pruning_error_no_archive_total` ticks so operators can see the signal that adding an archive upstream would help.

Relevant Prometheus metrics:

- `ebeacon_upstream_archive{network,upstream}` — 1 if archive, 0 if pruned
- `ebeacon_archive_promotion_total{network,reason}` — counter, `reason` is `proactive` or `pruning_error`
- `ebeacon_pruning_error_no_archive_total{network}` — counter of pruning-shaped 404s returned because no archive upstream exists

The bundled Grafana overview dashboard has an **Archive Routing** row covering all three.

When hedge is enabled (`failsafe.hedge`) and proactive archive routing fires, multiple parallel requests go to archive upstreams simultaneously. For metered providers like QuickNode or Alchemy, either disable hedge for that network or set per-upstream `rateLimiting.autoTune` to stay inside your plan.

## Caching

Responses are cached per network using path patterns and TTLs. **Finality-aware promotion**: when a request targets data at a **finalized** slot (derived from network finality checkpoints), the effective TTL is promoted so finalized data can be cached effectively forever regardless of a shorter policy TTL. **Unfinalized** head-dependent data uses the configured TTL so clients do not see stale head state for long.

Cache behavior is representation-aware:

- transport encoding is interchangeable: cached responses can be served as plain or `gzip` depending on `Accept-Encoding`
- JSON and SSZ / `application/octet-stream` are not interchangeable: they use distinct cache keys because eBeacon does not transcode between those representations

Backends:

- **memory** — LRU with `maxSize` entries (default driver).
- **redis** — shared cache across proxy instances; configure `cache.driver: redis` and `cache.redis.url`.

## Architecture

Traffic flows through layered components:

### Request Flow

Client → Proxy / HTTP router → Network context

For each request, roughly:

1. **Blocked paths** — regex denylist (e.g. debug, pool submission) if configured.
2. **Rate limiting** — per-IP and optional global token buckets.
3. **Routing rules** — Dugtrio-style `clientRoutes`, ordered `routeRules`, sticky sessions, load-balancing pick.
4. **Cache lookup** — on cacheable methods/paths, return if hit.
5. **Execute** — multiplex identical in-flight requests; apply retry, hedge, circuit breaker, optional consensus policy; forward to chosen upstream(s).
6. **Response** — preserve Ethereum consensus headers, optional gzip, SSE handling where applicable.

## Comparison with Dugtrio

| Feature                | eBeacon              | Dugtrio |
| ---------------------- | -------------------- | ------- |
| Retry/Hedge            | Yes                  | No      |
| Response caching       | Yes (memory + Redis) | No      |
| Score-based routing    | Yes                  | No      |
| Request multiplexing   | Yes                  | No      |
| Circuit breaker        | Yes                  | No      |
| Consensus verification | Yes                  | No      |
| Rate limit auto-tuner  | Yes                  | No      |
| Fork detection         | Yes                  | Yes     |
| Client auto-detection  | Yes                  | Yes     |
| Web UI                 | Yes                  | Yes     |
| gzip compression       | Yes                  | No      |
| Horizontal scaling     | Yes (Redis)          | No      |

## Acknowledgements

eBeacon was inspired by [eRPC](https://github.com/erpc/erpc) (Apache 2.0) and [Dugtrio](https://github.com/ethpandaops/dugtrio). See the [NOTICE](NOTICE) file for full attribution details.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
