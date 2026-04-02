# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

**eBeacon** is a fault-tolerant reverse proxy for Ethereum Beacon API nodes written in Go. It provides load balancing, response caching, canonical fork detection, request multiplexing, and multi-upstream reliability for Ethereum consensus layer (CL) infrastructure.

## Commands

```bash
make fmt           # Format Go files with gofmt
make fmt-check     # Verify formatting (used in CI)
make lint          # Run golangci-lint
make test          # Run Go tests
make vet           # Run go vet
make ci-local      # Run full CI suite (fmt-check + vet + lint + test + vuln)
make docker-build  # Build Docker image

go build -o ebeacon .
./ebeacon -config ebeacon.yaml
```

Run a single test package:

```bash
go test ./upstream/...
go test -run TestFunctionName ./network/...
```

Load and reliability testing scripts:

```bash
go run ./scripts/loadtest/ -base http://127.0.0.1:5555/mainnet -concurrency 50 -duration 60
go run ./scripts/reliability/ -duration 30m -report 1m
```

## Architecture

**Request flow:**

```text
Client → proxy/ (HTTP router, auth, rate limiting)
       → network/ (routing, consensus verification, SSE relay)
       → cache/ (LRU memory or Redis)
       → upstream/ (health scoring, failsafe, request execution)
       → Beacon nodes (Lighthouse, Prysm, Teku, Nimbus, etc.)
```

**Key packages:**

- **`proxy/`** — HTTP entry point. Routes by network name prefix (e.g., `/mainnet/...`), enforces auth tiers (`auth.go`, `auth_ratelimit.go`), deduplicates concurrent identical requests.
- **`network/`** — Per-network orchestration. `routing.go` selects upstreams, `consensus.go` does majority-vote fork detection, `session.go` manages client state, `sse.go` relays Server-Sent Events with reconnect/dedup.
- **`upstream/`** — Upstream lifecycle. `pool.go` manages the upstream set, `health.go` runs periodic health checks, `scoring.go` computes selection scores, `ratelimit.go` enforces per-upstream limits, `blockcache.go` caches block-level data.
- **`cache/`** — Two backends: `memory.go` (LRU) and `redis.go`. Finality-aware TTL: cached responses get promoted to longer TTLs once a slot is finalized.
- **`config/`** — YAML parsing and validation. Single `Config` struct mirrors `ebeacon.example.yaml`.
- **`state/`** — Shared state for horizontal scaling. `local.go` for single-instance, `redis.go` for multi-instance via pub/sub.
- **`api/`** — Web UI and JSON status endpoints.
- **`reqctx/`** — Request context helpers passed through the call chain.

**Configuration** is YAML-based. See `ebeacon.example.yaml` for all options with comments. Networks are configured as named pools under the `networks:` key, each with upstreams, routing strategy, caching, and failsafe settings.

**Failsafe strategies** (configured per-network): retry with backoff/jitter, hedged requests (fire multiple, take first response), and per-upstream circuit breakers.

**Load balancing strategies**: score-based (default), round-robin, random, least-connections.

## Development Setup

The repo includes a `docker-compose.yml` with eBeacon + Prometheus (port 19090) + Grafana (port 13000). Copy `.env.example` to `.env` for Grafana credentials.

Git hooks in `.githooks/` run gofmt, tests, and lint on commit/push. Install with `make hooks-install`.

Grafana dashboard JSON lives in `monitoring/grafana/`.
