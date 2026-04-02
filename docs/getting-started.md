# Getting Started

This guide gets eBeacon running locally with a multi-upstream Beacon API pool.

## Prerequisites

- Go 1.26+
- At least one reachable Beacon API upstream (`http://.../eth/v1/...`)

## 1) Create a config

Copy the example and tailor upstream URLs:

```bash
cp ebeacon.example.yaml ebeacon.yaml
```

Or start with a minimal config:

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
    routing:
      loadBalancing: score
      stickySession: true
    cache:
      enabled: true
      maxSize: 4096
```

## 2) Build and run

```bash
go build -o ebeacon .
./ebeacon -config ebeacon.yaml
```

The proxy listens on `server.host:server.port` (default `0.0.0.0:5555`).

## 3) Send test requests

Multi-network mode:

```bash
curl "http://localhost:5555/mainnet/eth/v1/node/version"
```

If exactly one network is configured, prefix-free routes also work:

```bash
curl "http://localhost:5555/eth/v1/node/version"
```

## 4) Check observability endpoints

- Global health summary: `GET /healthz`
- Per-network health summary: `GET /mainnet/healthz`
- Synthetic beacon node health: `GET /mainnet/eth/v1/node/health`
- Prometheus metrics: `GET /metrics` (if `metrics.enabled: true`)
- Web UI: `GET /webui/` (if `ui.enabled: true`)
- Web UI API: `/webui/api/health`, `/webui/api/upstreams`, `/webui/api/cache`, `/webui/api/cache/entries`, `/webui/api/sessions`, `/webui/api/forks`

The dashboard includes a cache entries table for browsing recent cached responses, previewing cached bodies on demand, and deleting a specific entry by key. The same delete operation is available over the protected API with `DELETE /webui/api/cache/entries?network=<id>&key=<cache-key>`.

When you use client-route prefixes such as `/mainnet/lighthouse/eth/v1/node/health`, eBeacon scopes the synthetic health response to that selected upstream subset.

## 5) Docker (optional)

```bash
docker build -t ebeacon .
docker run -p 5555:5555 -v $(pwd)/ebeacon.yaml:/app/ebeacon.yaml ebeacon
```
