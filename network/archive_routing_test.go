package network

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ebeacon/ebeacon/config"
)

// seedHeadSlot advances the pool's block cache so that MaxSlot() returns a
// realistic "current head" value. Proactive archive classification needs this
// to compare target slots against a retention window.
func seedHeadSlot(t *testing.T, n *Network, slot uint64) {
	t.Helper()
	for _, u := range n.pool.All() {
		n.pool.BlockCache().AddBlock(u.ID, slot, fmt.Sprintf("0xhead%d", slot), "0xparent")
	}
}

func buildArchiveTestConfig(t *testing.T, id, prunedURL, archiveURL string, withArchive bool) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	archiveEntry := ""
	if withArchive {
		archiveEntry = fmt.Sprintf(`      - id: archive
        url: %q
        priority: 10
        archive: true
`, archiveURL)
	}
	yaml := fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe:
  timeout: { duration: 10s }
  retry: { maxAttempts: 3, delay: 1ms, backoff: 1, maxDelay: 1ms }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: pruned
        url: %q
        priority: 0
%s    routing:
      loadBalancing: round-robin
      stickySession: false
    cache:
      enabled: false
`, id, prunedURL, archiveEntry)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// Scenario A: Proactive archive routing — request for an old blob sidecar goes
// straight to the archive upstream on the first attempt, bypassing the pruned
// upstream entirely.
func TestArchiveRouting_ProactiveForOldBlob(t *testing.T) {
	var prunedHits, archiveHits atomic.Int64

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prunedHits.Add(1)
		if strings.Contains(r.URL.Path, "blob_sidecars") {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer pruned.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		archiveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":[{"from":"archive"}]}`))
	}))
	defer archive.Close()

	id := netID(t)
	cfg := buildArchiveTestConfig(t, id, pruned.URL, archive.URL, true)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Head at slot 20,000,000. Blob retention threshold ~131,072 slots (~18 days).
	// Target slot 100 is well inside the archive-required range.
	const headSlot = 20_000_000
	const oldSlot = 100
	seedHeadSlot(t, n, headSlot)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/eth/v1/beacon/blob_sidecars/%d", oldSlot), nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "archive") {
		t.Fatalf("body should have come from archive: %q", rec.Body.String())
	}
	if prunedHits.Load() != 0 {
		t.Fatalf("pruned upstream should not have been hit for proactive archive request, got %d hits", prunedHits.Load())
	}
	if archiveHits.Load() != 1 {
		t.Fatalf("archive should have been hit exactly once, got %d", archiveHits.Load())
	}
}

// Scenario B: Error-driven fallthrough — request for a block by root (slot
// unknown, proactive classification can't fire) hits pruned first, gets a 404,
// and the retry layer promotes to archive.
func TestArchiveRouting_ErrorDrivenForBlockByRoot(t *testing.T) {
	var prunedHits, archiveHits atomic.Int64

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prunedHits.Add(1)
		http.NotFound(w, r)
	}))
	defer pruned.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		archiveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"from":"archive"}}`))
	}))
	defer archive.Close()

	id := netID(t)
	cfg := buildArchiveTestConfig(t, id, pruned.URL, archive.URL, true)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	seedHeadSlot(t, n, 20_000_000)

	req := httptest.NewRequest(http.MethodGet, "/eth/v2/beacon/blocks/0xabcdef0123456789", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "archive") {
		t.Fatalf("body should have come from archive: %q", rec.Body.String())
	}
	if prunedHits.Load() != 1 {
		t.Fatalf("pruned should have been tried exactly once, got %d", prunedHits.Load())
	}
	if archiveHits.Load() != 1 {
		t.Fatalf("archive should have served after promotion, got %d", archiveHits.Load())
	}
}

// Scenario C: No archive upstream configured — pruning-shaped 404 returns
// unchanged to the client, no retries, no behavior change from pre-archive.
func TestArchiveRouting_NoArchivePropagates404(t *testing.T) {
	var prunedHits atomic.Int64

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prunedHits.Add(1)
		http.NotFound(w, r)
	}))
	defer pruned.Close()

	id := netID(t)
	cfg := buildArchiveTestConfig(t, id, pruned.URL, "", false)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	seedHeadSlot(t, n, 20_000_000)

	req := httptest.NewRequest(http.MethodGet, "/eth/v2/beacon/blocks/0xabcdef0123456789", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status should be 404, got %d body %q", rec.Code, rec.Body.String())
	}
	// Without archive upstreams, the pruning-shaped 404 is returned directly —
	// no retries, so exactly one upstream hit.
	if got := prunedHits.Load(); got != 1 {
		t.Fatalf("pruned should have been hit exactly once, got %d", got)
	}
}

// Scenario: Proactive archive routing must NOT be defeated by a route-rule
// preferID that pins the client to a pruned upstream. Without the
// preferID-neutralization in executeFS, ensurePreferredUpstreamFirst would
// prepend the pruned upstream to the archive-only candidate set, the retry
// loop would hit pruned first, and archiveBiased=true would skip the
// error-driven fallthrough — leaving the client with a false 404.
func TestArchiveRouting_PreferIDDoesNotDefeatProactive(t *testing.T) {
	var prunedHits, archiveHits atomic.Int64

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prunedHits.Add(1)
		http.NotFound(w, r)
	}))
	defer pruned.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		archiveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"from":"archive"}}`))
	}))
	defer archive.Close()

	id := netID(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	// routeRules with a regex pin every beacon API request to the pruned
	// upstream via preferID — simulating the stickySession / route-rule
	// scenario that previously defeated proactive archive routing.
	yaml := fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe:
  timeout: { duration: 10s }
  retry: { maxAttempts: 3, delay: 1ms, backoff: 1, maxDelay: 1ms }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: pruned
        url: %q
        priority: 0
      - id: archive
        url: %q
        priority: 10
        archive: true
    routing:
      loadBalancing: round-robin
      stickySession: false
      routeRules:
        - pathPattern: "^/eth/"
          upstreamId: pruned
    cache:
      enabled: false
`, id, pruned.URL, archive.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Head at 20M, target blob sidecar at slot 100 — firmly in archive-only
	// territory (18-day blob retention is ~131k slots).
	seedHeadSlot(t, n, 20_000_000)

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blob_sidecars/100", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("proactive archive should have served OK despite preferID pin to pruned: got %d body %q", rec.Code, rec.Body.String())
	}
	if prunedHits.Load() != 0 {
		t.Fatalf("preferID-neutralization failed: pruned upstream was hit %d times for proactively-archive-routed request", prunedHits.Load())
	}
	if archiveHits.Load() != 1 {
		t.Fatalf("archive should have been hit exactly once, got %d", archiveHits.Load())
	}
}

// Scenario: Archive upstream also returns 404 on a pruning-shaped request.
// We must NOT infinite-loop (dedup via triedByID) and must surface the 404 to
// the client after exhausting the archive candidates.
func TestArchiveRouting_ArchiveAlso404sCleanlyPropagates(t *testing.T) {
	var prunedHits, archiveHits atomic.Int64

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prunedHits.Add(1)
		http.NotFound(w, r)
	}))
	defer pruned.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		archiveHits.Add(1)
		http.NotFound(w, r)
	}))
	defer archive.Close()

	id := netID(t)
	cfg := buildArchiveTestConfig(t, id, pruned.URL, archive.URL, true)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	seedHeadSlot(t, n, 20_000_000)

	// By-root: proactive classification can't fire (slot unknown), so this
	// takes the error-driven fallthrough path, exercising the dedup logic
	// when the promoted archive set also 404s.
	req := httptest.NewRequest(http.MethodGet, "/eth/v2/beacon/blocks/0xabc123def456", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after both pruned and archive 404, got %d body %q", rec.Code, rec.Body.String())
	}
	// Must terminate: pruned tried (errored-driven entry point) and archive
	// tried at least once. Never infinite.
	if prunedHits.Load() == 0 {
		t.Fatalf("pruned should have been tried at least once, got 0")
	}
	if archiveHits.Load() == 0 {
		t.Fatalf("archive should have been tried at least once via error-driven promotion, got 0")
	}
	// Upper bound: we don't want to have hammered either upstream more than
	// the retry budget (maxAttempts=3 in the helper config).
	if prunedHits.Load()+archiveHits.Load() > 5 {
		t.Fatalf("total upstream hits %d exceeds reasonable retry budget (pruned=%d archive=%d)", prunedHits.Load()+archiveHits.Load(), prunedHits.Load(), archiveHits.Load())
	}
}

// Scenario D: Named-head request (e.g. /eth/v1/beacon/headers/head) that 404s
// is NOT treated as pruning — pruned nodes serve head/finalized fine, so a 404
// there just means the node is behind and we should NOT promote to archive.
func TestArchiveRouting_NamedHead404NotPromoted(t *testing.T) {
	var prunedHits, archiveHits atomic.Int64

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prunedHits.Add(1)
		http.NotFound(w, r)
	}))
	defer pruned.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		archiveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"from":"archive"}}`))
	}))
	defer archive.Close()

	id := netID(t)
	cfg := buildArchiveTestConfig(t, id, pruned.URL, archive.URL, true)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	seedHeadSlot(t, n, 20_000_000)

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/headers/head", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	// The 404 on /headers/head is not pruning-shaped; the client sees 404 and
	// archive is not hit. (Pruned is hit, potentially multiple times due to
	// generic retry settings, but archive stays at 0.)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body %q", rec.Code, rec.Body.String())
	}
	if archiveHits.Load() != 0 {
		t.Fatalf("archive must not be hit for named-head 404, got %d", archiveHits.Load())
	}
}

// Scenario E: Hedge fires two upstreams in parallel; the faster one returns a
// pruning-shaped 404, but the slower one returns 200. The 200 must win —
// i.e., the pruning response must be buffered, not immediately returned.
// Regression guard for executeHedgeFS's pruningResp buffer logic.
func TestArchiveRouting_HedgePruningDoesNotWinRace(t *testing.T) {
	var prunedHits, archiveHits atomic.Int64

	// Pruned responds immediately with a 404. Archive responds slowly with 200.
	// Without the pruning buffer, the fast 404 would win the hedge and the
	// client would see 404.
	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prunedHits.Add(1)
		http.NotFound(w, r)
	}))
	defer pruned.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		archiveHits.Add(1)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"from":"archive"}}`))
	}))
	defer archive.Close()

	id := netID(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	// Hedge fires 2 upstreams with a very short delay (1ms), so both are
	// in-flight within the test's tolerance.
	yaml := fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe:
  timeout: { duration: 10s }
  retry: { maxAttempts: 2, delay: 1ms, backoff: 1, maxDelay: 1ms }
  hedge: { delay: 1ms, maxCount: 1 }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: pruned
        url: %q
        priority: 0
      - id: archive
        url: %q
        priority: 10
        archive: true
    routing:
      loadBalancing: round-robin
      stickySession: false
    cache:
      enabled: false
`, id, pruned.URL, archive.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	seedHeadSlot(t, n, 20_000_000)

	// By-root so proactive classification can't fire — both upstreams are
	// eligible for hedge, racing.
	req := httptest.NewRequest(http.MethodGet, "/eth/v2/beacon/blocks/0xdeadbeef", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("archive 200 should have won the hedge race: got status %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "archive") {
		t.Fatalf("body should have come from archive, got %q", rec.Body.String())
	}
}

// Scenario F: Client-type-pinned routing (X-Ebeacon-Use-Upstream) with two
// upstreams of the same type where the priority-0 one returns pruning-shaped
// 404. The loop must fall through to the priority-10 archive-capable peer
// instead of returning the 404. Regression guard for executeSelectedCandidatesFS.
func TestArchiveRouting_ClientTypePinnedFallthrough(t *testing.T) {
	var prunedHits, archiveHits atomic.Int64

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/eth/v1/node/version") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":"Lighthouse/v8.1.3/x86_64-linux"}}`))
			return
		}
		prunedHits.Add(1)
		http.NotFound(w, r)
	}))
	defer pruned.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/eth/v1/node/version") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":"Lighthouse/v8.1.3/x86_64-linux"}}`))
			return
		}
		archiveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"from":"archive"}}`))
	}))
	defer archive.Close()

	id := netID(t)
	cfg := buildArchiveTestConfig(t, id, pruned.URL, archive.URL, true)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	// The health watcher's version probe identifies both as Lighthouse. We
	// can't wait for the background watcher cleanly in a test, so set the
	// client type directly on each upstream.
	for _, u := range n.pool.All() {
		u.SetClientType("lighthouse")
	}

	seedHeadSlot(t, n, 20_000_000)

	// Client-type-pinned GET by block root — triggers executeSelectedFS path.
	// Pruned (priority 0) returns 404; the loop must fall through to archive.
	req := httptest.NewRequest(http.MethodGet, "/eth/v2/beacon/blocks/0xfeedface", nil)
	req.Header.Set("X-Ebeacon-Use-Upstream", "lighthouse")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("client-type-pinned pruning-shaped 404 should have promoted to archive: got %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "archive") {
		t.Fatalf("body should have come from archive: %q", rec.Body.String())
	}
	if prunedHits.Load() != 1 {
		t.Fatalf("pruned should have been tried exactly once, got %d", prunedHits.Load())
	}
	if archiveHits.Load() != 1 {
		t.Fatalf("archive should have served after fallthrough, got %d", archiveHits.Load())
	}
}

// Scenario G: PeerDAS 400 custody error is recognized as pruning-shaped and
// promoted to an archive upstream. The 400 has a body containing the
// "Insufficient data columns to reconstruct blobs" signal that Lighthouse
// emits on non-supernode deployments post-Fusaka. End-to-end check that
// peekBodyForPruning + isPruningError + promotion flow together.
func TestArchiveRouting_PeerDAS400Promotes(t *testing.T) {
	var prunedHits, archiveHits atomic.Int64

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prunedHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":400,"message":"BAD_REQUEST: Insufficient data columns to reconstruct blobs: required 64, but only 0 were found. You may need to run the beacon node with --supernode or --semi-supernode.","stacktraces":[]}`))
	}))
	defer pruned.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		archiveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":[{"from":"archive"}]}`))
	}))
	defer archive.Close()

	id := netID(t)
	cfg := buildArchiveTestConfig(t, id, pruned.URL, archive.URL, true)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Target a blob sidecar by root so proactive classification stays off —
	// exercises the error-driven path, which is where the 400 body-peek
	// classification has to do the work.
	seedHeadSlot(t, n, 20_000_000)

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blob_sidecars/0xdeadbabe", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PeerDAS 400 should have promoted to archive: got status %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "archive") {
		t.Fatalf("body should have come from archive: %q", rec.Body.String())
	}
	if archiveHits.Load() != 1 {
		t.Fatalf("archive should have been hit exactly once, got %d", archiveHits.Load())
	}
}
