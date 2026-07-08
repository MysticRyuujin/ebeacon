// Package main implements a long-running reliability test for eBeacon.
//
// It validates three properties continuously:
//
//  1. Correctness — eBeacon responses for immutable data (genesis, config)
//     are byte-identical to direct upstream responses.
//  2. Cache accuracy — when eBeacon returns a cache HIT for a specific slot,
//     the body matches what the upstream returns for that same slot.
//  3. SSE health — long-running event-stream connections receive head/finality
//     events at expected intervals and with monotonically increasing slot numbers.
//
// pprof snapshots of eBeacon are captured automatically at a configurable interval
// and saved to a directory. A final report is printed on exit.
//
// Usage:
//
//	go run ./scripts/reliability/ [flags]
//	go run ./scripts/reliability/ -duration 2h -pprof-every 15m
//
// Flags:
//
//	-ebeacon      eBeacon base URL (default: http://127.0.0.1:5555/mainnet)
//	-upstream     Comma-separated direct upstream URLs to validate against
//	-pprof        eBeacon pprof base URL (default: http://localhost:6060)
//	-pprof-dir    Directory for pprof output files (default: /tmp/ebeacon-reliability)
//	-pprof-every  How often to collect a pprof snapshot (default: 10m)
//	-duration     Total run time; 0 means run until SIGINT (default: 30m)
//	-report       Progress report interval (default: 1m)
//	-auth         Bearer token for Authorization header
//	-api-key      value for X-API-Key header
//	-timeout      Per-request timeout (default: 15s)
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ── configuration ─────────────────────────────────────────────────────────────

type config struct {
	ebeaconBase  string
	upstreamURLs []string
	metricsURL   string
	pprofAddr    string
	pprofDir     string
	pprofEvery   time.Duration
	duration     time.Duration
	reportEvery  time.Duration
	auth         string
	apiKey       string
	timeout      time.Duration
}

func loadConfig() config {
	ebeacon := flag.String("ebeacon", "http://127.0.0.1:5555/mainnet", "eBeacon base URL including network prefix")
	upstream := flag.String("upstream", "http://localhost:5052", "comma-separated direct upstream URLs")
	metrics := flag.String("metrics", "", "eBeacon Prometheus metrics URL (default: derived from -ebeacon; empty string after explicit '-metrics off' disables)")
	pprofAddr := flag.String("pprof", "http://localhost:6060", "eBeacon pprof base URL")
	pprofDir := flag.String("pprof-dir", "/tmp/ebeacon-reliability", "directory for pprof output files")
	pprofEvery := flag.Duration("pprof-every", 10*time.Minute, "interval between pprof snapshots")
	duration := flag.Duration("duration", 30*time.Minute, "total run duration (0 = until SIGINT)")
	reportEvery := flag.Duration("report", time.Minute, "progress report interval")
	auth := flag.String("auth", "", "Bearer token for Authorization header")
	apiKey := flag.String("api-key", "", "value for X-API-Key header")
	timeout := flag.Duration("timeout", 15*time.Second, "per-request HTTP timeout")
	flag.Parse()

	cfg := config{
		ebeaconBase: strings.TrimRight(*ebeacon, "/"),
		pprofAddr:   strings.TrimRight(*pprofAddr, "/"),
		pprofDir:    *pprofDir,
		pprofEvery:  *pprofEvery,
		duration:    *duration,
		reportEvery: *reportEvery,
		auth:        *auth,
		apiKey:      *apiKey,
		timeout:     *timeout,
	}

	// Env vars override flags.
	if v := os.Getenv("EBEACON_BASE"); v != "" {
		cfg.ebeaconBase = strings.TrimRight(v, "/")
	}
	if v := os.Getenv("EBEACON_AUTH"); v != "" {
		cfg.auth = v
	}
	if v := os.Getenv("EBEACON_API_KEY"); v != "" {
		cfg.apiKey = v
	}

	for _, u := range strings.Split(*upstream, ",") {
		if u = strings.TrimSpace(u); u != "" {
			cfg.upstreamURLs = append(cfg.upstreamURLs, strings.TrimRight(u, "/"))
		}
	}

	switch *metrics {
	case "off":
	case "":
		if u, err := url.Parse(cfg.ebeaconBase); err == nil && u.Host != "" {
			u.Path = "/metrics"
			cfg.metricsURL = u.String()
		}
	default:
		cfg.metricsURL = *metrics
	}
	return cfg
}

// ── atomic counters ────────────────────────────────────────────────────────────

type counters struct {
	// Immutable-data checks
	immutableChecks     atomic.Int64
	immutableMismatches atomic.Int64

	// Cache accuracy checks
	cacheHITs          atomic.Int64
	cacheMISSes        atomic.Int64
	cacheAccuracyFails atomic.Int64 // HIT body didn't match upstream for same slot

	// Cache freshness (slot-advancement over time)
	freshnessChecks atomic.Int64
	freshnessStalls atomic.Int64 // head slot not advancing

	// Encoding compatibility
	encodingChecks   atomic.Int64
	encodingFailures atomic.Int64

	// SSE
	sseConnects    atomic.Int64
	sseReconnects  atomic.Int64
	sseEvents      atomic.Int64
	sseGaps        atomic.Int64 // gap > 2 slots between head events
	sseReorderings atomic.Int64 // slot went backwards

	// pprof
	pprofSnapshots atomic.Int64
	pprofErrors    atomic.Int64

	// Upstream reachability
	upstreamErrors atomic.Int64

	// eBeacon-side fetch failures (error or non-200). Without this counter a
	// proxy that errors on every request produces zero checks and a green
	// report.
	ebeaconErrors atomic.Int64

	// Metrics invariants (active-connections quiesce check)
	metricsChecks atomic.Int64
	metricsFails  atomic.Int64
}

// ── mismatch log ──────────────────────────────────────────────────────────────

type mismatch struct {
	at       time.Time
	kind     string
	endpoint string
	detail   string
}

type mismatchLog struct {
	mu      sync.Mutex
	entries []mismatch
}

func (m *mismatchLog) add(kind, endpoint, detail string) {
	m.mu.Lock()
	m.entries = append(m.entries, mismatch{at: time.Now(), kind: kind, endpoint: endpoint, detail: detail})
	m.mu.Unlock()
	slog.Warn("mismatch", "kind", kind, "endpoint", endpoint, "detail", detail)
}

func (m *mismatchLog) snapshot() []mismatch {
	m.mu.Lock()
	out := make([]mismatch, len(m.entries))
	copy(out, m.entries)
	m.mu.Unlock()
	return out
}

// ── pprof file tracker ────────────────────────────────────────────────────────

type pprofFiles struct {
	mu    sync.Mutex
	paths []string
}

func (p *pprofFiles) add(path string) {
	p.mu.Lock()
	p.paths = append(p.paths, path)
	p.mu.Unlock()
}

func (p *pprofFiles) all() []string {
	p.mu.Lock()
	out := make([]string, len(p.paths))
	copy(out, p.paths)
	p.mu.Unlock()
	return out
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout}
}

type fetchResult struct {
	status   int
	body     []byte
	cacheHit bool   // true when X-Ebeacon-Cache: HIT
	upstream string // X-Ebeacon-Upstream header value
}

type fetchOptions struct {
	accept         string
	auth           string
	apiKey         string
	acceptEncoding string
}

func fetch(ctx context.Context, client *http.Client, url, auth, apiKey string) (fetchResult, error) {
	return fetchWithOptions(ctx, client, url, fetchOptions{accept: "application/json", auth: auth, apiKey: apiKey})
}

func fetchWithOptions(ctx context.Context, client *http.Client, url string, opts fetchOptions) (fetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fetchResult{}, err
	}
	accept := opts.accept
	if accept == "" {
		accept = "application/json"
	}
	req.Header.Set("Accept", accept)
	if opts.acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", opts.acceptEncoding)
	}
	if opts.auth != "" {
		req.Header.Set("Authorization", "Bearer "+opts.auth)
	}
	if opts.apiKey != "" {
		req.Header.Set("X-API-Key", opts.apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fetchResult{}, err
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fetchResult{}, err
	}
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return fetchResult{}, err
		}
		decoded, err := io.ReadAll(gr)
		gr.Close() //nolint:errcheck
		if err != nil {
			return fetchResult{}, err
		}
		body = decoded
	}
	return fetchResult{
		status:   resp.StatusCode,
		body:     body,
		cacheHit: resp.Header.Get("X-Ebeacon-Cache") == "HIT",
		upstream: resp.Header.Get("X-Ebeacon-Upstream"),
	}, nil
}

// recordEbeaconError counts an eBeacon-side fetch failure. Shutdown
// cancellations are excluded so a clean exit doesn't register as proxy errors.
func recordEbeaconError(ctx context.Context, c *counters, checker, endpoint string, status int, err error) {
	if ctx.Err() != nil {
		return
	}
	c.ebeaconErrors.Add(1)
	slog.Warn("eBeacon fetch failed", "checker", checker, "endpoint", endpoint, "status", status, "err", err)
}

// normalizeJSON round-trips JSON to produce a canonical (sorted-key) representation
// so that key-order differences don't count as mismatches.
func normalizeJSON(b []byte) ([]byte, error) {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// extractSlot attempts to pull a slot number from a JSON response.
// It checks common beacon API shapes: data.header.message.slot, data.message.slot, data.slot.
func extractSlot(body []byte) (uint64, bool) {
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return 0, false
	}
	data, _ := raw["data"].(map[string]interface{})
	if data == nil {
		return 0, false
	}
	candidates := []interface{}{
		data["slot"],
		func() interface{} {
			if h, ok := data["header"].(map[string]interface{}); ok {
				if m, ok := h["message"].(map[string]interface{}); ok {
					return m["slot"]
				}
			}
			return nil
		}(),
		func() interface{} {
			if m, ok := data["message"].(map[string]interface{}); ok {
				return m["slot"]
			}
			return nil
		}(),
	}
	for _, c := range candidates {
		switch v := c.(type) {
		case string:
			if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
				return n, true
			}
		case float64:
			if uint64(v) > 0 {
				return uint64(v), true
			}
		}
	}
	return 0, false
}

// ── immutable-data checker ────────────────────────────────────────────────────
// These endpoints must return byte-identical JSON across eBeacon and all upstreams.

var immutableEndpoints = []string{
	"/eth/v1/beacon/genesis",
	// config/spec, config/fork_schedule, and config/deposit_contract are excluded:
	// different consensus clients return different field sets or address formats,
	// so a mismatch is expected and not a proxy correctness issue.
}

func runImmutableChecks(ctx context.Context, cfg config, c *counters, log *mismatchLog) {
	client := newHTTPClient(cfg.timeout)
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	check := func() {
		for _, ep := range immutableEndpoints {
			ebRes, err := fetch(ctx, client, cfg.ebeaconBase+ep, cfg.auth, cfg.apiKey)
			if err != nil || ebRes.status != http.StatusOK {
				recordEbeaconError(ctx, c, "immutable", ep, ebRes.status, err)
				continue
			}
			ebNorm, err := normalizeJSON(ebRes.body)
			if err != nil {
				continue
			}

			for _, upURL := range cfg.upstreamURLs {
				upRes, err := fetch(ctx, client, upURL+ep, "", "")
				if err != nil || upRes.status != http.StatusOK {
					c.upstreamErrors.Add(1)
					continue
				}
				upNorm, err := normalizeJSON(upRes.body)
				if err != nil {
					continue
				}

				c.immutableChecks.Add(1)
				if !bytes.Equal(ebNorm, upNorm) {
					c.immutableMismatches.Add(1)
					log.add("immutable-mismatch", ep, fmt.Sprintf("eBeacon vs %s differ", upURL))
				}
			}
		}
	}

	// Run once immediately at startup, then on ticker.
	check()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

// ── cache accuracy checker ────────────────────────────────────────────────────
// Fetches head-relative endpoints from eBeacon, extracts the slot, then fetches
// that specific slot directly from an upstream. Cache HITs must match.

var cacheAccuracyEndpoints = []struct {
	ebeaconPath string // head-relative, hits eBeacon cache
	upstreamFmt string // slot-specific, fetched directly from upstream; %s = slot
}{
	{
		ebeaconPath: "/eth/v1/beacon/headers/head",
		upstreamFmt: "/eth/v1/beacon/headers/%s",
	},
	{
		ebeaconPath: "/eth/v1/beacon/blocks/head/root",
		upstreamFmt: "/eth/v1/beacon/blocks/%s/root",
	},
	{
		ebeaconPath: "/eth/v1/beacon/states/head/root",
		upstreamFmt: "/eth/v1/beacon/states/%s/root",
	},
	{
		ebeaconPath: "/eth/v1/beacon/states/head/finality_checkpoints",
		upstreamFmt: "/eth/v1/beacon/states/%s/finality_checkpoints",
	},
}

func runCacheAccuracyChecks(ctx context.Context, cfg config, c *counters, log *mismatchLog) {
	if len(cfg.upstreamURLs) == 0 {
		slog.Warn("cache accuracy: no upstream URLs configured, skipping")
		return
	}
	client := newHTTPClient(cfg.timeout)
	// 30s ticker for the main "first fetch + upstream comparison" pass.
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	check := func() {
		upURL := cfg.upstreamURLs[0] // use first upstream as reference

		for _, ep := range cacheAccuracyEndpoints {
			// First fetch: head endpoints have a 12s TTL so this is almost always a MISS
			// when polled every 30s (head slot has changed by then).
			ebRes, err := fetch(ctx, client, cfg.ebeaconBase+ep.ebeaconPath, cfg.auth, cfg.apiKey)
			if err != nil || ebRes.status != http.StatusOK {
				recordEbeaconError(ctx, c, "cache-accuracy", ep.ebeaconPath, ebRes.status, err)
				continue
			}
			if ebRes.cacheHit {
				c.cacheHITs.Add(1)
			} else {
				c.cacheMISSes.Add(1)
			}

			// Immediate re-fetch: the first fetch just populated the cache, so this
			// should be a HIT within the same TTL window (< 1s elapsed).
			ebRes2, err := fetch(ctx, client, cfg.ebeaconBase+ep.ebeaconPath, cfg.auth, cfg.apiKey)
			if err != nil || ebRes2.status != http.StatusOK {
				recordEbeaconError(ctx, c, "cache-accuracy", ep.ebeaconPath, ebRes2.status, err)
				continue
			}
			if ebRes2.cacheHit {
				c.cacheHITs.Add(1)
			} else {
				c.cacheMISSes.Add(1)
				log.add("cache-miss-after-miss", ep.ebeaconPath,
					"immediate re-fetch was not a HIT; cache may not be enabled or entry was evicted")
			}

			// Extract slot from the first response and verify accuracy against upstream.
			slot, ok := extractSlot(ebRes.body)
			if !ok {
				continue
			}
			slotStr := strconv.FormatUint(slot, 10)

			upPath := fmt.Sprintf(ep.upstreamFmt, slotStr)
			upRes, err := fetch(ctx, client, upURL+upPath, "", "")
			if err != nil || upRes.status != http.StatusOK {
				c.upstreamErrors.Add(1)
				continue
			}

			ebNorm, err1 := normalizeJSON(ebRes.body)
			upNorm, err2 := normalizeJSON(upRes.body)
			if err1 != nil || err2 != nil {
				continue
			}

			if !bytes.Equal(ebNorm, upNorm) {
				c.cacheAccuracyFails.Add(1)
				cacheLabel := "MISS"
				if ebRes.cacheHit {
					cacheLabel = "HIT"
				}
				log.add("cache-accuracy", ep.ebeaconPath,
					fmt.Sprintf("slot=%s cache=%s eBeacon≠upstream(%s)", slotStr, cacheLabel, upURL))
			}
		}
	}

	check()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

// ── cache freshness checker ───────────────────────────────────────────────────
// Verifies that head-relative cached entries don't get "stuck" — the slot
// numbers returned by eBeacon must advance over time.

func runCacheFreshnessChecks(ctx context.Context, cfg config, c *counters, log *mismatchLog) {
	client := newHTTPClient(cfg.timeout)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	const ep = "/eth/v1/beacon/headers/head"
	// TTL for beacon/headers is 12s. After 3× that we expect the slot to have changed.
	const staleAfter = 3 * 12 * time.Second

	type observation struct {
		slot   uint64
		seenAt time.Time
	}
	var last observation

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		res, err := fetch(ctx, client, cfg.ebeaconBase+ep, cfg.auth, cfg.apiKey)
		if err != nil || res.status != http.StatusOK {
			recordEbeaconError(ctx, c, "cache-freshness", ep, res.status, err)
			continue
		}
		slot, ok := extractSlot(res.body)
		if !ok {
			continue
		}

		c.freshnessChecks.Add(1)
		now := time.Now()

		if last.slot == 0 {
			last = observation{slot, now}
			continue
		}

		if slot > last.slot {
			last = observation{slot, now}
			continue
		}

		// Slot hasn't advanced.
		staleFor := now.Sub(last.seenAt)
		if staleFor > staleAfter {
			c.freshnessStalls.Add(1)
			log.add("cache-freshness", ep,
				fmt.Sprintf("slot %d stuck for %s (expected to advance after %s)",
					slot, staleFor.Round(time.Second), staleAfter))
		}
	}
}

// ── encoding compatibility checker ───────────────────────────────────────────
// Verifies that cache hits can be served as gzip or plain without changing the
// decoded payload. Representation changes like JSON <-> SSZ are intentionally
// not treated as interchangeable.

func runEncodingCompatibilityChecks(ctx context.Context, cfg config, c *counters, log *mismatchLog) {
	client := newHTTPClient(cfg.timeout)
	ticker := time.NewTicker(45 * time.Second)
	defer ticker.Stop()

	checkPath := func(label, path, accept string) {
		sequences := []struct {
			first  string
			second string
		}{
			{first: "gzip", second: "identity"},
			{first: "identity", second: "gzip"},
		}
		for _, seq := range sequences {
			first, err := fetchWithOptions(ctx, client, cfg.ebeaconBase+path, fetchOptions{
				accept:         accept,
				auth:           cfg.auth,
				apiKey:         cfg.apiKey,
				acceptEncoding: seq.first,
			})
			if err != nil || first.status != http.StatusOK {
				recordEbeaconError(ctx, c, "encoding-compat", path, first.status, err)
				continue
			}
			second, err := fetchWithOptions(ctx, client, cfg.ebeaconBase+path, fetchOptions{
				accept:         accept,
				auth:           cfg.auth,
				apiKey:         cfg.apiKey,
				acceptEncoding: seq.second,
			})
			if err != nil || second.status != http.StatusOK {
				recordEbeaconError(ctx, c, "encoding-compat", path, second.status, err)
				continue
			}

			c.encodingChecks.Add(1)
			if !bytes.Equal(first.body, second.body) {
				c.encodingFailures.Add(1)
				log.add("encoding-compat", label,
					fmt.Sprintf("decoded body mismatch for %s -> %s", seq.first, seq.second))
				continue
			}
			if !second.cacheHit {
				c.encodingFailures.Add(1)
				log.add("encoding-compat", label,
					fmt.Sprintf("expected cache HIT for %s -> %s sequence", seq.first, seq.second))
			}
		}
	}

	resolveStableBinaryPath := func() (string, error) {
		res, err := fetch(ctx, client, cfg.ebeaconBase+"/eth/v1/beacon/headers/head", cfg.auth, cfg.apiKey)
		if err != nil {
			return "", err
		}
		if res.status != http.StatusOK {
			return "", fmt.Errorf("headers/head returned %d", res.status)
		}
		slot, ok := extractSlot(res.body)
		if !ok {
			return "", fmt.Errorf("could not extract slot from headers/head")
		}
		if slot > 0 {
			slot--
		}
		return "/eth/v2/beacon/blocks/" + strconv.FormatUint(slot, 10), nil
	}

	check := func() {
		checkPath("json:/eth/v1/node/version", "/eth/v1/node/version", "application/json")
		if path, err := resolveStableBinaryPath(); err == nil {
			checkPath("binary:"+path, path, "application/octet-stream")
		}
	}

	check()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

// ── SSE monitor ───────────────────────────────────────────────────────────────
// Maintains a persistent SSE connection and validates event health.

type sseEvent struct {
	eventType string
	data      string
}

func parseSseEvent(raw string) sseEvent {
	var ev sseEvent
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "event:") {
			ev.eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			ev.data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
	return ev
}

// runSSEMonitor subscribes to head+finalized_checkpoint events and tracks health.
func runSSEMonitor(ctx context.Context, cfg config, c *counters, log *mismatchLog) {
	topics := "head,finalized_checkpoint,chain_reorg"
	url := cfg.ebeaconBase + "/eth/v1/events?topics=" + topics

	// maxHeadInterval: two consecutive slots without a head event is a gap.
	const slotDuration = 12 * time.Second
	const maxHeadInterval = 3 * slotDuration

	var (
		lastHeadSlot   uint64
		lastHeadAt     time.Time
		lastFinalEpoch uint64
	)

	for ctx.Err() == nil {
		c.sseConnects.Add(1)
		slog.Info("sse: connecting", "url", url)

		err := streamSSE(ctx, cfg, url, func(ev sseEvent) {
			c.sseEvents.Add(1)

			switch ev.eventType {
			case "head":
				var payload struct {
					Slot string `json:"slot"`
				}
				if json.Unmarshal([]byte(ev.data), &payload) != nil {
					return
				}
				slot, err := strconv.ParseUint(payload.Slot, 10, 64)
				if err != nil {
					return
				}

				now := time.Now()

				// Check for gap in events (missed slots).
				if !lastHeadAt.IsZero() {
					if gap := now.Sub(lastHeadAt); gap > maxHeadInterval {
						missed := int(gap/slotDuration) - 1
						c.sseGaps.Add(1)
						log.add("sse-gap", "/eth/v1/events?topics=head",
							fmt.Sprintf("%.0fs gap after slot %d (~%d missed slots)",
								gap.Seconds(), lastHeadSlot, missed))
					}

					// Check for reordering (slot went backwards).
					if slot < lastHeadSlot {
						c.sseReorderings.Add(1)
						log.add("sse-reorder", "/eth/v1/events?topics=head",
							fmt.Sprintf("slot went %d → %d", lastHeadSlot, slot))
					}
				}

				lastHeadSlot = slot
				lastHeadAt = now

			case "finalized_checkpoint":
				var payload struct {
					Epoch string `json:"epoch"`
				}
				if json.Unmarshal([]byte(ev.data), &payload) != nil {
					return
				}
				epoch, err := strconv.ParseUint(payload.Epoch, 10, 64)
				if err != nil {
					return
				}
				if epoch < lastFinalEpoch {
					log.add("sse-finality-reorder", "/eth/v1/events?topics=finalized_checkpoint",
						fmt.Sprintf("finalized epoch went %d → %d", lastFinalEpoch, epoch))
				}
				lastFinalEpoch = epoch
			}
		})

		if ctx.Err() != nil {
			return
		}
		slog.Warn("sse: disconnected, reconnecting", "err", err)
		c.sseReconnects.Add(1)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// streamSSE opens an SSE connection and calls onEvent for each complete event.
// Returns when the connection closes or ctx is cancelled.
func streamSSE(ctx context.Context, cfg config, url string, onEvent func(sseEvent)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if cfg.auth != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.auth)
	}
	if cfg.apiKey != "" {
		req.Header.Set("X-API-Key", cfg.apiKey)
	}

	client := &http.Client{} // no timeout on the outer client for SSE
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	var buf strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			// Empty line signals end of event.
			if buf.Len() > 0 {
				onEvent(parseSseEvent(buf.String()))
				buf.Reset()
			}
			continue
		}
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}

// ── metrics invariants ────────────────────────────────────────────────────────
// Scrapes eBeacon's Prometheus endpoint for lifecycle-accounting invariants
// that per-request checks can't see, e.g. the hedge-loser leak where
// active_connections drifted upward forever.

func scrapeActiveConns(ctx context.Context, client *http.Client, metricsURL string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	const metric = "ebeacon_upstream_active_connections{"
	out := make(map[string]float64)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, metric) {
			continue
		}
		labelsEnd := strings.IndexByte(line, '}')
		if labelsEnd < 0 {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(line[labelsEnd+1:]), 64)
		if err != nil {
			continue
		}
		out[line[len(metric):labelsEnd]] = v
	}
	return out, scanner.Err()
}

// runMetricsWatchdog samples active connections during the run (Warn-level
// trend signal only; harness traffic makes instantaneous values noisy).
// The authoritative check is checkQuiescedActiveConns after traffic stops.
func runMetricsWatchdog(ctx context.Context, cfg config, c *counters) {
	if cfg.metricsURL == "" {
		return
	}
	client := newHTTPClient(cfg.timeout)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		conns, err := scrapeActiveConns(ctx, client, cfg.metricsURL)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("metrics: scrape failed", "url", cfg.metricsURL, "err", err)
			}
			continue
		}
		c.metricsChecks.Add(1)
		for upstream, v := range conns {
			if v > 8 {
				slog.Warn("metrics: high active connections (possible leak)", "labels", upstream, "value", v)
			}
		}
	}
}

// checkQuiescedActiveConns runs after all harness traffic (including the SSE
// stream) has stopped: every upstream's active-connection gauge must be zero,
// or response lifecycle accounting leaked.
func checkQuiescedActiveConns(cfg config, c *counters, log *mismatchLog) {
	if cfg.metricsURL == "" {
		return
	}
	time.Sleep(2 * time.Second)
	client := newHTTPClient(cfg.timeout)
	conns, err := scrapeActiveConns(context.Background(), client, cfg.metricsURL)
	if err != nil {
		slog.Warn("metrics: final quiesce scrape failed", "url", cfg.metricsURL, "err", err)
		return
	}
	c.metricsChecks.Add(1)
	for upstream, v := range conns {
		if v != 0 {
			c.metricsFails.Add(1)
			log.add("metrics-active-conns-leak", "ebeacon_upstream_active_connections",
				fmt.Sprintf("{%s} = %g after quiesce; expected 0", upstream, v))
		}
	}
}

// ── pprof collector ───────────────────────────────────────────────────────────
// Fetches heap snapshots, CPU profiles, and goroutine dumps from eBeacon's
// pprof endpoint. Also captures its own goroutine profile to help detect
// leaks in the test itself.

func runPprofCollector(ctx context.Context, cfg config, c *counters, files *pprofFiles) {
	if err := os.MkdirAll(cfg.pprofDir, 0755); err != nil {
		slog.Error("pprof: cannot create output dir", "dir", cfg.pprofDir, "err", err)
		return
	}

	// Check pprof is reachable.
	probe := &http.Client{Timeout: 5 * time.Second}
	if _, err := probe.Get(cfg.pprofAddr + "/debug/pprof/"); err != nil {
		slog.Warn("pprof: eBeacon pprof not reachable, skipping remote snapshots", "addr", cfg.pprofAddr, "err", err)
	}

	// Capture initial heap and goroutines at startup.
	captureHeap(ctx, cfg, c, files)
	captureEBeaconGoroutines(ctx, cfg, c, files, "startup")
	captureSelf(cfg, files, "startup")

	ticker := time.NewTicker(cfg.pprofEvery)
	defer ticker.Stop()

	snap := 1
	for {
		select {
		case <-ctx.Done():
			// Final snapshots on shutdown.
			captureHeap(context.Background(), cfg, c, files)
			captureCPU(context.Background(), cfg, c, files, 5*time.Second, snap)
			captureEBeaconGoroutines(context.Background(), cfg, c, files, "shutdown")
			captureSelf(cfg, files, "shutdown")
			return
		case <-ticker.C:
			captureHeap(ctx, cfg, c, files)
			captureCPU(ctx, cfg, c, files, 30*time.Second, snap)
			captureEBeaconGoroutines(ctx, cfg, c, files, fmt.Sprintf("snap%02d", snap))
			captureSelf(cfg, files, fmt.Sprintf("snap%02d", snap))
			snap++
		}
	}
}

func captureHeap(ctx context.Context, cfg config, c *counters, files *pprofFiles) {
	tag := time.Now().Format("20060102-150405")
	path := filepath.Join(cfg.pprofDir, "heap-"+tag+".pprof")
	if err := fetchPprofProfile(ctx, cfg.pprofAddr+"/debug/pprof/heap", path); err != nil {
		c.pprofErrors.Add(1)
		slog.Warn("pprof: heap capture failed", "err", err)
		return
	}
	files.add(path)
	c.pprofSnapshots.Add(1)
	slog.Info("pprof: heap snapshot saved", "file", path)
}

func captureCPU(ctx context.Context, cfg config, c *counters, files *pprofFiles, dur time.Duration, snap int) {
	tag := time.Now().Format("20060102-150405")
	path := filepath.Join(cfg.pprofDir, fmt.Sprintf("cpu-%s-snap%02d.pprof", tag, snap))
	url := fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", cfg.pprofAddr, int(dur.Seconds()))
	slog.Info("pprof: starting CPU profile", "duration", dur)
	if err := fetchPprofProfile(ctx, url, path); err != nil {
		c.pprofErrors.Add(1)
		slog.Warn("pprof: CPU capture failed", "err", err)
		return
	}
	files.add(path)
	c.pprofSnapshots.Add(1)
	slog.Info("pprof: CPU profile saved", "file", path)
}

// captureSelf writes this process's own goroutine dump (useful for leak detection in the test).
func captureSelf(cfg config, files *pprofFiles, tag string) {
	path := filepath.Join(cfg.pprofDir, "goroutines-self-"+tag+".txt")
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close() //nolint:errcheck
	if err := pprof.Lookup("goroutine").WriteTo(f, 1); err != nil {
		return
	}
	files.add(path)
	slog.Info("pprof: self goroutine dump saved", "file", path)
}

// captureEBeaconGoroutines fetches eBeacon's goroutine dump from its remote
// pprof endpoint. Unlike captureSelf (which dumps this test process's own
// goroutines), this captures the actual goroutines running inside eBeacon.
func captureEBeaconGoroutines(ctx context.Context, cfg config, c *counters, files *pprofFiles, tag string) {
	path := filepath.Join(cfg.pprofDir, "goroutines-ebeacon-"+tag+".txt")
	if err := fetchPprofText(ctx, cfg.pprofAddr+"/debug/pprof/goroutine?debug=1", path); err != nil {
		c.pprofErrors.Add(1)
		slog.Warn("pprof: eBeacon goroutine capture failed", "err", err)
		return
	}
	files.add(path)
	slog.Info("pprof: eBeacon goroutine dump saved", "file", path)
}

// fetchPprofText fetches a pprof text endpoint (e.g. goroutine?debug=1) and saves
// the response body as a plain text file.
func fetchPprofText(ctx context.Context, url, outPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(outPath, data, 0644)
}

func fetchPprofProfile(ctx context.Context, url, outPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 90 * time.Second} // CPU profiles can take up to 30s + overhead
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(outPath, data, 0644)
}

// ── reporter ──────────────────────────────────────────────────────────────────

func printReport(c *counters, start time.Time) {
	elapsed := time.Since(start).Round(time.Second)
	fmt.Printf("\n── eBeacon Reliability Report (%s elapsed) ─────────────────────────\n", elapsed)
	fmt.Printf("  Immutable checks:        %d  mismatches: %d\n",
		c.immutableChecks.Load(), c.immutableMismatches.Load())
	fmt.Printf("  Cache HITs:              %d  accuracy fails: %d\n",
		c.cacheHITs.Load(), c.cacheAccuracyFails.Load())
	fmt.Printf("  Encoding checks:         %d  failures: %d\n",
		c.encodingChecks.Load(), c.encodingFailures.Load())
	fmt.Printf("  Cache MISSes:            %d\n", c.cacheMISSes.Load())
	fmt.Printf("  Cache freshness checks:  %d  stalls: %d\n",
		c.freshnessChecks.Load(), c.freshnessStalls.Load())
	fmt.Printf("  SSE events:              %d  gaps: %d  reorders: %d  reconnects: %d\n",
		c.sseEvents.Load(), c.sseGaps.Load(), c.sseReorderings.Load(), c.sseReconnects.Load())
	fmt.Printf("  eBeacon errors:          %d\n", c.ebeaconErrors.Load())
	fmt.Printf("  Upstream errors:         %d\n", c.upstreamErrors.Load())
	fmt.Printf("  pprof snapshots:         %d  errors: %d\n",
		c.pprofSnapshots.Load(), c.pprofErrors.Load())
}

func printFinalReport(cfg config, c *counters, log *mismatchLog, files *pprofFiles, start time.Time) bool {
	elapsed := time.Since(start).Round(time.Second)
	fmt.Println()
	fmt.Println("══════════════════════════════════════════════════════════════════════")
	fmt.Printf("  eBeacon Reliability Test — Final Report (runtime: %s)\n", elapsed)
	fmt.Println("══════════════════════════════════════════════════════════════════════")

	total := c.immutableMismatches.Load() + c.cacheAccuracyFails.Load() +
		c.encodingFailures.Load() + c.freshnessStalls.Load() + c.sseGaps.Load() +
		c.sseReorderings.Load() + c.metricsFails.Load()

	// Zero completed checks means the run proved nothing — a proxy that
	// errors on every request must not produce a green report. Only enforced
	// past 2 minutes so brief smoke runs don't trip on slow tickers.
	var zeroActivity []string
	if elapsed >= 2*time.Minute {
		if len(cfg.upstreamURLs) > 0 {
			if c.immutableChecks.Load() == 0 {
				zeroActivity = append(zeroActivity, "immutable")
			}
			if c.cacheHITs.Load()+c.cacheMISSes.Load() == 0 {
				zeroActivity = append(zeroActivity, "cache-accuracy")
			}
		}
		if c.encodingChecks.Load() == 0 {
			zeroActivity = append(zeroActivity, "encoding")
		}
		if c.freshnessChecks.Load() == 0 {
			zeroActivity = append(zeroActivity, "freshness")
		}
		if c.sseEvents.Load() == 0 {
			zeroActivity = append(zeroActivity, "sse")
		}
	}

	passed := total == 0 && len(zeroActivity) == 0 && c.ebeaconErrors.Load() == 0

	switch {
	case len(zeroActivity) > 0:
		fmt.Printf("  ✗ FAIL: checker(s) completed zero checks: %s\n", strings.Join(zeroActivity, ", "))
	case total > 0:
		fmt.Printf("  ✗ FAIL: %d total issue(s) detected\n", total)
	case c.ebeaconErrors.Load() > 0:
		fmt.Printf("  ✗ FAIL: %d eBeacon request error(s) during the run\n", c.ebeaconErrors.Load())
	default:
		fmt.Println("  ✓ No correctness issues detected")
	}
	fmt.Println()

	fmt.Println("  Correctness")
	fmt.Printf("    Immutable checks:      %d  (mismatches: %d)\n",
		c.immutableChecks.Load(), c.immutableMismatches.Load())
	fmt.Printf("    Cache accuracy checks: %d  (fails: %d)\n",
		c.cacheHITs.Load()+c.cacheMISSes.Load(), c.cacheAccuracyFails.Load())
	fmt.Printf("    Encoding checks:       %d  (fails: %d)\n",
		c.encodingChecks.Load(), c.encodingFailures.Load())
	fmt.Printf("    Cache HITs / MISSes:   %d / %d\n",
		c.cacheHITs.Load(), c.cacheMISSes.Load())
	total_cache := c.cacheHITs.Load() + c.cacheMISSes.Load()
	if total_cache > 0 {
		fmt.Printf("    Hit rate:              %.1f%%\n",
			float64(c.cacheHITs.Load())*100/float64(total_cache))
	}
	fmt.Println()

	fmt.Println("  Cache Freshness")
	fmt.Printf("    Checks:                %d  (stalls: %d)\n",
		c.freshnessChecks.Load(), c.freshnessStalls.Load())
	fmt.Println()

	fmt.Println("  SSE Streaming")
	fmt.Printf("    Total events:          %d\n", c.sseEvents.Load())
	fmt.Printf("    Connects:              %d\n", c.sseConnects.Load())
	fmt.Printf("    Reconnects:            %d\n", c.sseReconnects.Load())
	fmt.Printf("    Slot gaps detected:    %d\n", c.sseGaps.Load())
	fmt.Printf("    Slot reorderings:      %d\n", c.sseReorderings.Load())
	fmt.Println()

	fmt.Println("  Errors / Metrics")
	fmt.Printf("    eBeacon errors:        %d\n", c.ebeaconErrors.Load())
	fmt.Printf("    Upstream errors:       %d\n", c.upstreamErrors.Load())
	fmt.Printf("    Metrics scrapes:       %d  (invariant fails: %d)\n",
		c.metricsChecks.Load(), c.metricsFails.Load())
	fmt.Println()

	mismatches := log.snapshot()
	if len(mismatches) > 0 {
		fmt.Println("  Issues detected:")
		// Group by kind.
		byKind := make(map[string][]mismatch)
		for _, m := range mismatches {
			byKind[m.kind] = append(byKind[m.kind], m)
		}
		kinds := make([]string, 0, len(byKind))
		for k := range byKind {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		for _, kind := range kinds {
			fmt.Printf("    [%s] (%d occurrences)\n", kind, len(byKind[kind]))
			for _, m := range byKind[kind] {
				fmt.Printf("      %s  %s — %s\n",
					m.at.Format("15:04:05"), m.endpoint, m.detail)
			}
		}
		fmt.Println()
	}

	pprofPaths := files.all()
	if len(pprofPaths) > 0 {
		fmt.Printf("  pprof files (%d saved to %s):\n", len(pprofPaths), pprofPaths[0][:strings.LastIndex(pprofPaths[0], "/")])
		for _, p := range pprofPaths {
			name := filepath.Base(p)
			if strings.HasSuffix(name, ".pprof") {
				fmt.Printf("    go tool pprof -http=:8080 %s\n", p)
			} else {
				fmt.Printf("    cat %s\n", p)
			}
		}
	}
	fmt.Println("══════════════════════════════════════════════════════════════════════")
	return passed
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	slog.Info("eBeacon reliability test starting",
		"ebeacon", cfg.ebeaconBase,
		"upstreams", len(cfg.upstreamURLs),
		"duration", cfg.duration,
		"pprof_every", cfg.pprofEvery,
		"pprof_dir", cfg.pprofDir)

	// Verify eBeacon is reachable.
	probe := newHTTPClient(10 * time.Second)
	if r, err := fetch(context.Background(), probe, cfg.ebeaconBase+"/eth/v1/node/version", cfg.auth, cfg.apiKey); err != nil || r.status != http.StatusOK {
		fmt.Fprintf(os.Stderr, "ERROR: cannot reach eBeacon at %s: %v\n", cfg.ebeaconBase, err)
		os.Exit(1)
	}
	slog.Info("eBeacon reachable")

	// Log which upstreams are reachable (non-fatal if some are not).
	for _, u := range cfg.upstreamURLs {
		r, err := fetch(context.Background(), probe, u+"/eth/v1/node/version", "", "")
		if err != nil || r.status != http.StatusOK {
			slog.Warn("upstream not reachable — accuracy checks will skip it", "url", u, "err", err)
		} else {
			slog.Info("upstream reachable", "url", u)
		}
	}

	// Root context: cancelled either by duration or SIGINT.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.duration > 0 {
		go func() {
			select {
			case <-time.After(cfg.duration):
				slog.Info("duration elapsed, shutting down")
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-quit:
			slog.Info("signal received, shutting down", "signal", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	var (
		c      counters
		mlog   mismatchLog
		pfiles pprofFiles
	)

	start := time.Now()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		runImmutableChecks(ctx, cfg, &c, &mlog)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runCacheAccuracyChecks(ctx, cfg, &c, &mlog)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runCacheFreshnessChecks(ctx, cfg, &c, &mlog)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runEncodingCompatibilityChecks(ctx, cfg, &c, &mlog)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runSSEMonitor(ctx, cfg, &c, &mlog)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runPprofCollector(ctx, cfg, &c, &pfiles)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runMetricsWatchdog(ctx, cfg, &c)
	}()

	// Periodic progress reporter.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(cfg.reportEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				printReport(&c, start)
			}
		}
	}()

	wg.Wait()
	checkQuiescedActiveConns(cfg, &c, &mlog)
	if !printFinalReport(cfg, &c, &mlog, &pfiles, start) {
		os.Exit(1)
	}
}
