// eBeacon load tester — generates synthetic Beacon API traffic including SSE streams.
//
// Usage:
//
//	go run ./scripts/loadtest/
//	go run ./scripts/loadtest/ -base http://127.0.0.1:5555/mainnet -concurrency 50 -duration 60
//
// Env vars (override flags):
//
//	EBEACON_BASE       — base URL including network prefix, e.g. http://host:port/mainnet
//	EBEACON_AUTH       — Bearer token value (sent as "Authorization: Bearer <value>")
//	EBEACON_API_KEY    — value sent as "X-API-Key" header
//	CONCURRENCY        — parallel HTTP workers (default 30)
//	SSE_WORKERS        — parallel SSE streaming workers (default 1)
//	DURATION           — seconds each worker runs (default 120)
//	ERROR_PCT          — share of requests hitting a bogus path (default 0)
//	REQUEST_TIMEOUT    — per-request HTTP timeout in seconds (default 60)
//	REPORT_INTERVAL    — seconds between progress prints (default 10)
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ── configuration ────────────────────────────────────────────────────────────

type config struct {
	base           string
	auth           string
	apiKey         string
	concurrency    int
	sseWorkers     int
	duration       time.Duration
	errorPct       int
	requestTimeout time.Duration
	reportInterval time.Duration
}

func loadConfig() config {
	base := flag.String("base", "http://127.0.0.1:5555/mainnet", "eBeacon base URL including network prefix")
	auth := flag.String("auth", "", "Bearer token for Authorization header")
	apiKey := flag.String("api-key", "", "value for X-API-Key header")
	concurrency := flag.Int("concurrency", 30, "parallel HTTP workers")
	sseWorkers := flag.Int("sse-workers", 1, "parallel SSE streaming workers")
	duration := flag.Int("duration", 120, "seconds to run")
	errorPct := flag.Int("error-pct", 0, "percentage of requests sent to invalid paths")
	requestTimeout := flag.Int("request-timeout", 60, "per-request HTTP timeout in seconds")
	reportInterval := flag.Int("report-interval", 10, "seconds between progress reports")
	flag.Parse()

	cfg := config{
		base:           *base,
		auth:           *auth,
		apiKey:         *apiKey,
		concurrency:    *concurrency,
		sseWorkers:     *sseWorkers,
		duration:       time.Duration(*duration) * time.Second,
		errorPct:       *errorPct,
		requestTimeout: time.Duration(*requestTimeout) * time.Second,
		reportInterval: time.Duration(*reportInterval) * time.Second,
	}

	// Env vars override flags.
	if v := os.Getenv("EBEACON_BASE"); v != "" {
		cfg.base = strings.TrimRight(v, "/")
	}
	if v := os.Getenv("EBEACON_AUTH"); v != "" {
		cfg.auth = v
	}
	if v := os.Getenv("EBEACON_API_KEY"); v != "" {
		cfg.apiKey = v
	}
	if v := os.Getenv("CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.concurrency = n
		}
	}
	if v := os.Getenv("SSE_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.sseWorkers = n
		}
	}
	if v := os.Getenv("DURATION"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.duration = time.Duration(n) * time.Second
		}
	}
	if v := os.Getenv("ERROR_PCT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.errorPct = n
		}
	}
	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.requestTimeout = time.Duration(n) * time.Second
		}
	}
	if v := os.Getenv("REPORT_INTERVAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.reportInterval = time.Duration(n) * time.Second
		}
	}

	cfg.base = strings.TrimRight(cfg.base, "/")
	return cfg
}

// ── endpoint definitions ──────────────────────────────────────────────────────

type method string

const (
	methodGET  method = "GET"
	methodPOST method = "POST"
	methodHEAD method = "HEAD"
)

const (
	endpointHeadersByBlockID     = "/eth/v1/beacon/headers/{block_id}"
	endpointBlocksV2ByBlockID    = "/eth/v2/beacon/blocks/{block_id}"
	endpointBlockRootByBlockID   = "/eth/v1/beacon/blocks/{block_id}/root"
	endpointStateValidator       = "/eth/v1/beacon/states/{state_id}/validators/{validator_id}"
	endpointBeaconBlobs          = "/eth/v1/beacon/blobs/{block_id}"
	endpointNodeSyncing          = "/eth/v1/node/syncing"
	endpointEvents               = "/eth/v1/events"
	endpointHeadersList          = "/eth/v1/beacon/headers"
	endpointConfigSpec           = "/eth/v1/config/spec"
	endpointFinalityCheckpoints  = "/eth/v1/beacon/states/{state_id}/finality_checkpoints"
	endpointRewardsSyncCommittee = "/eth/v1/beacon/rewards/sync_committee/{block_id}"
	endpointNodeVersion          = "/eth/v1/node/version"
	endpointRewardsBlocks        = "/eth/v1/beacon/rewards/blocks/{block_id}"
	endpointRewardsAttestations  = "/eth/v1/beacon/rewards/attestations/{epoch}"
	endpointBeaconBlobSidecars   = "/eth/v1/beacon/blob_sidecars/{block_id}"
	endpointNodeHealth           = "/eth/v1/node/health"
)

type endpoint struct {
	name   string
	method method
	path   string
	body   interface{} // marshalled to JSON for POST; nil for GET/HEAD
	weight int
	accept string // overrides default "application/json" Accept header (e.g. "application/octet-stream" for SSZ)
}

// chainState holds live values fetched from the beacon node at startup.
type chainState struct {
	headSlot       uint64
	finalizedEpoch uint64
	finalizedSlot  uint64
	// prevEpoch is finalizedEpoch-1, used for duties endpoints so the epoch is
	// guaranteed to be in the past and fully computed by the node.
	prevEpoch uint64
}

// fetchChainState queries the beacon node for its current head slot and
// finalized epoch so that slot/epoch-parameterised endpoints use real values.
func fetchChainState(ctx context.Context, baseURL string, auth string, apiKey string) (chainState, error) {
	client := &http.Client{Timeout: 10 * time.Second}

	doGet := func(path string) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		if apiKey != "" {
			req.Header.Set("X-API-Key", apiKey)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GET %s: HTTP %d", path, resp.StatusCode)
		}
		return io.ReadAll(resp.Body)
	}

	// /eth/v1/beacon/states/head/finality_checkpoints gives us finalized epoch.
	body, err := doGet("/eth/v1/beacon/states/head/finality_checkpoints")
	if err != nil {
		return chainState{}, fmt.Errorf("finality_checkpoints: %w", err)
	}
	var cpResp struct {
		Data struct {
			Finalized struct {
				Epoch string `json:"epoch"`
			} `json:"finalized"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &cpResp); err != nil {
		return chainState{}, fmt.Errorf("parse finality_checkpoints: %w", err)
	}
	finalizedEpoch, err := strconv.ParseUint(cpResp.Data.Finalized.Epoch, 10, 64)
	if err != nil {
		return chainState{}, fmt.Errorf("parse finalized epoch %q: %w", cpResp.Data.Finalized.Epoch, err)
	}

	// /eth/v1/node/syncing gives us the head slot.
	body, err = doGet("/eth/v1/node/syncing")
	if err != nil {
		return chainState{}, fmt.Errorf("node/syncing: %w", err)
	}
	var syncResp struct {
		Data struct {
			HeadSlot string `json:"head_slot"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &syncResp); err != nil {
		return chainState{}, fmt.Errorf("parse syncing: %w", err)
	}
	headSlot, err := strconv.ParseUint(syncResp.Data.HeadSlot, 10, 64)
	if err != nil {
		return chainState{}, fmt.Errorf("parse head_slot %q: %w", syncResp.Data.HeadSlot, err)
	}

	prev := finalizedEpoch
	if prev > 1 {
		prev--
	}
	return chainState{
		headSlot:       headSlot,
		finalizedEpoch: finalizedEpoch,
		finalizedSlot:  finalizedEpoch*32 + 31,
		prevEpoch:      prev,
	}, nil
}

// buildEndpoints constructs a RealWorld route mix that tracks observed public Beacon API
// traffic. SSE is modeled separately via sseWorkers, so the HTTP weights below
// intentionally exclude /eth/v1/events.
func buildEndpoints(cs chainState) []endpoint {
	finalizedSlot := strconv.FormatUint(cs.finalizedSlot, 10)
	prevEpoch := strconv.FormatUint(cs.prevEpoch, 10)

	return []endpoint{
		{name: endpointHeadersByBlockID, method: methodGET, path: "/eth/v1/beacon/headers/head", weight: 20},
		{name: endpointHeadersByBlockID, method: methodGET, path: "/eth/v1/beacon/headers/finalized", weight: 6},
		{name: endpointHeadersByBlockID, method: methodGET, path: "/eth/v1/beacon/headers/" + finalizedSlot, weight: 6},

		{name: endpointBlocksV2ByBlockID, method: methodGET, path: "/eth/v2/beacon/blocks/head", weight: 14},
		{name: endpointBlocksV2ByBlockID, method: methodGET, path: "/eth/v2/beacon/blocks/finalized", weight: 4},
		{name: endpointBlocksV2ByBlockID, method: methodGET, path: "/eth/v2/beacon/blocks/" + finalizedSlot, weight: 4},

		{name: endpointBlockRootByBlockID, method: methodGET, path: "/eth/v1/beacon/blocks/head/root", weight: 8},
		{name: endpointBlockRootByBlockID, method: methodGET, path: "/eth/v1/beacon/blocks/finalized/root", weight: 2},
		{name: endpointBlockRootByBlockID, method: methodGET, path: "/eth/v1/beacon/blocks/" + finalizedSlot + "/root", weight: 2},

		// Full validator set queries are excluded — they return ~1M entries on
		// mainnet and routinely exceed any practical loadtest duration.

		{name: endpointStateValidator, method: methodGET, path: "/eth/v1/beacon/states/head/validators/1", weight: 3},
		{name: endpointStateValidator, method: methodGET, path: "/eth/v1/beacon/states/finalized/validators/1", weight: 2},
		{name: endpointStateValidator, method: methodGET, path: "/eth/v1/beacon/states/" + finalizedSlot + "/validators/1", weight: 1},

		{name: endpointBeaconBlobs, method: methodGET, path: "/eth/v1/beacon/blobs/head", weight: 2},
		{name: endpointBeaconBlobs, method: methodGET, path: "/eth/v1/beacon/blobs/finalized", weight: 1},
		{name: endpointBeaconBlobs, method: methodGET, path: "/eth/v1/beacon/blobs/" + finalizedSlot, weight: 1},

		{name: endpointNodeSyncing, method: methodGET, path: "/eth/v1/node/syncing", weight: 3},
		{name: endpointHeadersList, method: methodGET, path: "/eth/v1/beacon/headers", weight: 2},
		{name: endpointConfigSpec, method: methodGET, path: "/eth/v1/config/spec", weight: 1},
		{name: endpointFinalityCheckpoints, method: methodGET, path: "/eth/v1/beacon/states/head/finality_checkpoints", weight: 1},
		{name: endpointRewardsSyncCommittee, method: methodGET, path: "/eth/v1/beacon/rewards/sync_committee/" + finalizedSlot, weight: 1},
		{name: endpointNodeVersion, method: methodGET, path: "/eth/v1/node/version", weight: 1},
		{name: endpointRewardsBlocks, method: methodGET, path: "/eth/v1/beacon/rewards/blocks/" + finalizedSlot, weight: 1},
		{name: endpointRewardsAttestations, method: methodGET, path: "/eth/v1/beacon/rewards/attestations/" + prevEpoch, weight: 1},
		{name: endpointBeaconBlobSidecars, method: methodGET, path: "/eth/v1/beacon/blob_sidecars/" + finalizedSlot, weight: 1},
		{name: endpointNodeHealth, method: methodGET, path: "/eth/v1/node/health", weight: 1},
	}
}

// SSE topic subscriptions to test.
var sseTopics = []string{
	"head",
	"finalized_checkpoint",
	"chain_reorg",
	"block",
}

// ── latency tracking ──────────────────────────────────────────────────────────

type latHist struct {
	mu      sync.Mutex
	total   int64
	sum     float64 // ms
	samples []float64
}

func (h *latHist) observe(ms float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.total++
	h.sum += ms
	// Keep reservoir of up to 50k samples for exact percentiles.
	if len(h.samples) < 50000 {
		h.samples = append(h.samples, ms)
	} else {
		// Reservoir sampling.
		idx := rand.Intn(int(h.total))
		if idx < len(h.samples) {
			h.samples[idx] = ms
		}
	}
}

func (h *latHist) percentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.samples) == 0 {
		return 0
	}
	sorted := make([]float64, len(h.samples))
	copy(sorted, h.samples)
	sort.Float64s(sorted)
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (h *latHist) mean() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	return h.sum / float64(h.total)
}

// ── per-endpoint stats ────────────────────────────────────────────────────────

type stats struct {
	total  atomic.Int64
	errors atomic.Int64
	lat    latHist
}

// ── SSE stats ─────────────────────────────────────────────────────────────────

type sseStats struct {
	mu            sync.Mutex
	connects      int64
	reconnects    int64
	eventsTotal   int64
	byTopic       map[string]int64
	errors        int64
	activeWorkers atomic.Int64
}

func newSSEStats() *sseStats {
	return &sseStats{byTopic: make(map[string]int64)}
}

func (s *sseStats) addEvent(topic string) {
	s.mu.Lock()
	s.eventsTotal++
	s.byTopic[topic]++
	s.mu.Unlock()
}

// ── request helpers ───────────────────────────────────────────────────────────

func buildRequest(ctx context.Context, cfg config, ep endpoint) (*http.Request, error) {
	path := ep.path
	url := cfg.base + path

	var bodyReader io.Reader
	if ep.body != nil {
		b, err := json.Marshal(ep.body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, string(ep.method), url, bodyReader)
	if err != nil {
		return nil, err
	}

	if ep.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	accept := "application/json"
	if ep.accept != "" {
		accept = ep.accept
	}
	req.Header.Set("Accept", accept)
	if ep.method == methodGET || ep.method == methodHEAD {
		// Exercise both cache transport-encoding paths. The proxy should return the
		// same representation regardless of whether the client asks for gzip.
		if rand.Intn(2) == 0 {
			req.Header.Set("Accept-Encoding", "gzip")
		} else {
			req.Header.Set("Accept-Encoding", "identity")
		}
	}
	if cfg.auth != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.auth)
	}
	if cfg.apiKey != "" {
		req.Header.Set("X-API-Key", cfg.apiKey)
	}
	return req, nil
}

// ── weighted endpoint selection ───────────────────────────────────────────────

type weightedEndpoints struct {
	eps     []endpoint
	weights []int
	total   int
}

func buildWeighted(eps []endpoint) weightedEndpoints {
	w := weightedEndpoints{eps: eps}
	for _, e := range eps {
		wt := e.weight
		if wt <= 0 {
			wt = 1
		}
		w.weights = append(w.weights, wt)
		w.total += wt
	}
	return w
}

func (w *weightedEndpoints) pick() endpoint {
	r := rand.Intn(w.total)
	cum := 0
	for i, wt := range w.weights {
		cum += wt
		if r < cum {
			return w.eps[i]
		}
	}
	return w.eps[len(w.eps)-1]
}

// ── workers ───────────────────────────────────────────────────────────────────

func httpWorker(ctx context.Context, stopAt time.Time, cfg config, client *http.Client, weighted weightedEndpoints,
	statMap map[string]*stats, mu *sync.RWMutex, wid int) {

	for ctx.Err() == nil {
		if time.Now().After(stopAt) {
			return
		}
		ep := weighted.pick()

		// Inject random error paths according to errorPct.
		if rand.Intn(100) < cfg.errorPct {
			ep = endpoint{
				name:   "_error_path",
				method: methodGET,
				path:   fmt.Sprintf("/eth/v1/node/version/ebeacon-loadtest-miss-%d-%d", wid, rand.Int()),
				weight: 1,
			}
		}

		mu.RLock()
		st := statMap[ep.name]
		mu.RUnlock()
		if st == nil {
			mu.Lock()
			if statMap[ep.name] == nil {
				statMap[ep.name] = &stats{}
			}
			st = statMap[ep.name]
			mu.Unlock()
		}

		start := time.Now()
		req, err := buildRequest(ctx, cfg, ep)
		if err != nil {
			st.errors.Add(1)
			st.total.Add(1)
			continue
		}

		resp, err := client.Do(req)
		elapsed := time.Since(start).Seconds() * 1000 // ms

		st.total.Add(1)
		if err != nil || resp == nil {
			st.errors.Add(1)
		} else {
			// Drain body so connection can be reused.
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			resp.Body.Close()              //nolint:errcheck
			if resp.StatusCode >= 500 {
				st.errors.Add(1)
			}
			st.lat.observe(elapsed)
		}
	}
}

func sseWorker(ctx context.Context, cfg config, client *http.Client, topic string, ss *sseStats) {
	ss.activeWorkers.Add(1)
	defer ss.activeWorkers.Add(-1)

	url := cfg.base + "/eth/v1/events?topics=" + topic

	for ctx.Err() == nil {
		func() {
			ss.mu.Lock()
			ss.connects++
			ss.mu.Unlock()

			reqCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
			if err != nil {
				ss.mu.Lock()
				ss.errors++
				ss.mu.Unlock()
				return
			}
			req.Header.Set("Accept", "text/event-stream")
			req.Header.Set("Cache-Control", "no-cache")
			if cfg.auth != "" {
				req.Header.Set("Authorization", "Bearer "+cfg.auth)
			}
			if cfg.apiKey != "" {
				req.Header.Set("X-API-Key", cfg.apiKey)
			}

			resp, err := client.Do(req)
			if err != nil {
				if ctx.Err() == nil {
					ss.mu.Lock()
					ss.errors++
					ss.mu.Unlock()
				}
				return
			}
			defer resp.Body.Close() //nolint:errcheck

			if resp.StatusCode != http.StatusOK {
				ss.mu.Lock()
				ss.errors++
				ss.mu.Unlock()
				return
			}

			scanner := bufio.NewScanner(resp.Body)
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1<<20)

			for scanner.Scan() {
				if ctx.Err() != nil {
					return
				}
				line := scanner.Text()
				if strings.HasPrefix(line, "data:") {
					ss.addEvent(topic)
				}
			}
			// If the upstream closed, reconnect after a short pause.
			if ctx.Err() == nil {
				ss.mu.Lock()
				ss.reconnects++
				ss.mu.Unlock()
			}
		}()

		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ── reporting ─────────────────────────────────────────────────────────────────

type snapshot struct {
	name   string
	total  int64
	errors int64
	p50    float64
	p95    float64
	p99    float64
	mean   float64
}

func snapshotStats(statMap map[string]*stats, mu *sync.RWMutex) []snapshot {
	mu.RLock()
	defer mu.RUnlock()
	snaps := make([]snapshot, 0, len(statMap))
	for name, st := range statMap {
		snaps = append(snaps, snapshot{
			name:   name,
			total:  st.total.Load(),
			errors: st.errors.Load(),
			p50:    st.lat.percentile(50),
			p95:    st.lat.percentile(95),
			p99:    st.lat.percentile(99),
			mean:   st.lat.mean(),
		})
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].name < snaps[j].name })
	return snaps
}

func printProgress(statMap map[string]*stats, mu *sync.RWMutex, ss *sseStats, elapsed time.Duration) {
	snaps := snapshotStats(statMap, mu)

	var totalReqs, totalErrs int64
	for _, s := range snaps {
		totalReqs += s.total
		totalErrs += s.errors
	}

	rps := float64(totalReqs) / elapsed.Seconds()
	errRate := 0.0
	if totalReqs > 0 {
		errRate = float64(totalErrs) / float64(totalReqs) * 100
	}

	ss.mu.Lock()
	sseEvents := ss.eventsTotal
	sseConns := ss.connects
	sseReconns := ss.reconnects
	sseErrs := ss.errors
	ss.mu.Unlock()

	fmt.Printf("\n[%s elapsed] reqs=%d rps=%.1f err_rate=%.1f%%  sse_events=%d sse_conns=%d sse_reconnects=%d sse_errors=%d\n",
		elapsed.Round(time.Second), totalReqs, rps, errRate, sseEvents, sseConns, sseReconns, sseErrs)
}

func printFinalReport(statMap map[string]*stats, mu *sync.RWMutex, ss *sseStats, elapsed time.Duration) {
	snaps := snapshotStats(statMap, mu)

	var totalReqs, totalErrs int64
	for _, s := range snaps {
		totalReqs += s.total
		totalErrs += s.errors
	}

	rps := float64(totalReqs) / elapsed.Seconds()
	errRate := 0.0
	if totalReqs > 0 {
		errRate = float64(totalErrs) / float64(totalReqs) * 100
	}

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════════")
	fmt.Printf("  eBeacon load test — final report  (duration: %s)\n", elapsed.Round(time.Second))
	fmt.Println("═══════════════════════════════════════════════════════════════════════")
	fmt.Printf("  Total requests : %d\n", totalReqs)
	fmt.Printf("  Total errors   : %d (%.1f%%)\n", totalErrs, errRate)
	fmt.Printf("  Throughput     : %.1f req/s\n", rps)
	fmt.Println()

	// Per-endpoint table.
	fmt.Printf("  %-50s %8s %8s %8s %8s %8s %8s\n",
		"endpoint", "reqs", "errors", "mean(ms)", "p50(ms)", "p95(ms)", "p99(ms)")
	fmt.Println(" ", strings.Repeat("-", 106))
	for _, s := range snaps {
		errMark := ""
		if s.errors > 0 {
			errMark = " !"
		}
		fmt.Printf("  %-50s %8d %8d %8.1f %8.1f %8.1f %8.1f%s\n",
			s.name, s.total, s.errors, s.mean, s.p50, s.p95, s.p99, errMark)
	}

	// SSE summary.
	ss.mu.Lock()
	defer ss.mu.Unlock()
	fmt.Println()
	fmt.Println("  ── SSE streaming ──────────────────────────────────────")
	fmt.Printf("  Total events received : %d\n", ss.eventsTotal)
	fmt.Printf("  Connections opened    : %d\n", ss.connects)
	fmt.Printf("  Reconnects            : %d\n", ss.reconnects)
	fmt.Printf("  Errors                : %d\n", ss.errors)
	if len(ss.byTopic) > 0 {
		fmt.Println("  Events by topic:")
		topics := make([]string, 0, len(ss.byTopic))
		for t := range ss.byTopic {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		for _, t := range topics {
			fmt.Printf("    %-30s %d\n", t, ss.byTopic[t])
		}
	}
	fmt.Println("═══════════════════════════════════════════════════════════════════════")
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	fmt.Fprintf(os.Stderr, "eBeacon loadtest: base=%s concurrency=%d sse_workers=%d duration=%s error_pct=%d%% request_timeout=%s\n",
		cfg.base, cfg.concurrency, cfg.sseWorkers, cfg.duration, cfg.errorPct, cfg.requestTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration+cfg.requestTimeout)
	defer cancel()

	// Fetch live chain state so slot/epoch-parameterised endpoints use real values.
	fmt.Fprintf(os.Stderr, "fetching chain state from %s ...\n", cfg.base)
	cs, err := fetchChainState(ctx, cfg.base, cfg.auth, cfg.apiKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not fetch chain state (%v); epoch/slot endpoints may return 404\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "chain state: head_slot=%d finalized_epoch=%d finalized_slot=%d (using epoch=%d finalized slot for parameterised endpoints)\n",
			cs.headSlot, cs.finalizedEpoch, cs.finalizedSlot, cs.prevEpoch)
	}

	endpoints := buildEndpoints(cs)

	// Shared HTTP client for regular requests.
	httpClient := &http.Client{
		Timeout: cfg.requestTimeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: cfg.concurrency + cfg.sseWorkers + 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Long-lived client for SSE (no timeout on the connection itself).
	sseClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: cfg.sseWorkers + 5,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	weighted := buildWeighted(endpoints)
	statMap := make(map[string]*stats)
	var mu sync.RWMutex
	ss := newSSEStats()

	// Pre-populate stats map so the report is stable.
	for _, ep := range endpoints {
		statMap[ep.name] = &stats{}
	}
	statMap["_error_path"] = &stats{}

	// Start HTTP workers.
	stopAt := time.Now().Add(cfg.duration)
	workerCtx, workerCancel := context.WithTimeout(ctx, cfg.duration)
	defer workerCancel()

	var wg sync.WaitGroup
	for i := range cfg.concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			httpWorker(ctx, stopAt, cfg, httpClient, weighted, statMap, &mu, id)
		}(i)
	}

	// Start SSE workers, cycling through topics.
	for i := range cfg.sseWorkers {
		topic := sseTopics[i%len(sseTopics)]
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			sseWorker(workerCtx, cfg, sseClient, t, ss)
		}(topic)
	}

	// Progress reporter.
	start := time.Now()
	ticker := time.NewTicker(cfg.reportInterval)
	defer ticker.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	draining := false
loop:
	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start)
			printProgress(statMap, &mu, ss, elapsed)
			if !draining && elapsed >= cfg.duration {
				draining = true
				fmt.Fprintf(os.Stderr, "\nduration reached, draining in-flight requests (timeout: %s)...\n\n", cfg.requestTimeout)
			}
		case <-done:
			break loop
		}
	}

	printFinalReport(statMap, &mu, ss, time.Since(start))
}
