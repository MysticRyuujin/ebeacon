#!/usr/bin/env bash
# Synthetic traffic for eBeacon dashboards and Prometheus metrics.
#
# Usage:
#   ./scripts/loadtest.sh
#   EBEACON_BASE='http://127.0.0.1:5555/mainnet' CONCURRENCY=50 DURATION=60 ERROR_PCT=15 ./scripts/loadtest.sh
#
# EBEACON_BASE — Root URL including the network prefix, e.g.
#   http://host:port/mainnet
# CONCURRENCY — parallel workers, each sending requests for the whole run (default 30)
# DURATION — seconds each worker runs (default 120)
# ERROR_PCT — approximate share of requests that hit a non-existent path (404 / 4xx)
# VARIED_CACHE — set to 1 to rotate across several default cacheable paths (grows ebeacon_cache_size).
#   Default behavior hits only /eth/v1/node/version on success, so cache size stays at 1 entry.
#
# Most synthetic "errors" here are client errors (4xx). The Grafana "Error rate (5xx)" panel
# counts server-side failures only; use a failing upstream or real outages to exercise 5xx.
#
# "Active upstream connections" is an instantaneous gauge; identical concurrent GETs are merged
# (singleflight) and upstream calls finish quickly, so it often reads 0 between Prometheus scrapes.

set -euo pipefail

EBEACON_BASE="${EBEACON_BASE:-http://127.0.0.1:5555/mainnet}"
CONCURRENCY="${CONCURRENCY:-30}"
DURATION="${DURATION:-120}"
ERROR_PCT="${ERROR_PCT:-15}"
VARIED_CACHE="${VARIED_CACHE:-0}"

EBEACON_BASE="${EBEACON_BASE%/}"

# Paths that match eBeacon default cache policies (see cache/cache.go defaultPolicies).
_CACHE_PATHS=(
  "/eth/v1/node/version"
  "/eth/v1/node/syncing"
  "/eth/v1/node/peer_count"
)

worker() {
  local wid="$1"
  local end=$((SECONDS + DURATION))
  local n=0
  while (( SECONDS < end )); do
    local rnd=$((RANDOM % 100))
    local url
    if (( rnd < ERROR_PCT )); then
      url="${EBEACON_BASE}/eth/v1/node/version/ebeacon-loadtest-miss-${wid}-${n}"
    elif [[ "$VARIED_CACHE" == "1" ]]; then
      local i=$((n % ${#_CACHE_PATHS[@]}))
      url="${EBEACON_BASE}${_CACHE_PATHS[$i]}"
    else
      url="${EBEACON_BASE}/eth/v1/node/version"
    fi
    curl -sS -o /dev/null -w '' --connect-timeout 5 --max-time 30 "$url" || true
    n=$((n + 1))
  done
}

export EBEACON_BASE ERROR_PCT DURATION

echo "eBeacon loadtest: base=${EBEACON_BASE} concurrency=${CONCURRENCY} duration=${DURATION}s error_pct=${ERROR_PCT} varied_cache=${VARIED_CACHE}" >&2
for w in $(seq 1 "$CONCURRENCY"); do
  worker "$w" &
done
wait
echo "done." >&2
