package network

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHeadWatcher_InvalidatesNamedHeadKeysOnDataOnlyEOFEvent(t *testing.T) {
	t.Parallel()

	sseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "data: {\"slot\":\"100\"}") //nolint:errcheck
	}))
	defer sseServer.Close()

	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`
logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: u1
        url: %q
    cache: { enabled: true, maxSize: 32 }
`, id, sseServer.URL))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	headKey := id + ":GET:/eth/v1/beacon/headers/head"
	numericKey := id + ":GET:/eth/v1/beacon/headers/12345"
	n.cache.Set(headKey, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"stale":true}`), 4*time.Second)
	n.cache.Set(numericKey, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"slot":12345}`), 12*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	w := startHeadWatcher(ctx, id, n.pool, n.cache, nil)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n.cache.Get(headKey) == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if n.cache.Get(headKey) != nil {
		cancel()
		w.wait()
		t.Fatal("head cache entry was not invalidated after data-only SSE event")
	}
	if n.cache.Get(numericKey) == nil {
		cancel()
		w.wait()
		t.Fatal("numeric slot cache entry was unexpectedly invalidated")
	}

	cancel()
	w.wait()
}

func TestHeadWatcher_RecordsHeadBlockIntoBlockCache(t *testing.T) {
	t.Parallel()

	// Two upstream servers: both serve the SSE events endpoint, but only
	// upstream-b actually emits a head event. The test verifies that after
	// the watcher processes that event, ONLY upstream-b appears in the
	// canonical-head-seen set, so the pool's head-aware selector prefers it.
	const (
		headSlot  = "200"
		headBlock = "0xdeadbeef"
	)

	sseHandler := func(emitHead bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/eth/v1/events" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if emitHead {
				fmt.Fprintf(w, "event: head\ndata: {\"slot\":%q,\"block\":%q}\n\n", headSlot, headBlock) //nolint:errcheck
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			<-r.Context().Done()
		}
	}

	emitter := httptest.NewServer(sseHandler(true))
	defer emitter.Close()
	quiet := httptest.NewServer(sseHandler(false))
	defer quiet.Close()

	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`
logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: upstream-b
        url: %q
      - id: upstream-a
        url: %q
    cache: { enabled: true, maxSize: 32 }
`, id, emitter.URL, quiet.URL))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := startHeadWatcher(ctx, id, n.pool, n.cache, nil)
	defer func() {
		cancel()
		w.wait()
	}()

	// Wait for the head event to be processed and recorded into the BlockCache.
	deadline := time.Now().Add(3 * time.Second)
	var seen map[string]bool
	for time.Now().Before(deadline) {
		seen = n.pool.BlockCache().CanonicalHeadSeenBy()
		if seen["upstream-b"] {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if !seen["upstream-b"] {
		t.Fatalf("expected upstream-b to be recorded as a canonical head reporter, got %v", seen)
	}
	if seen["upstream-a"] {
		t.Fatalf("upstream-a did not emit a head event and must not appear: %v", seen)
	}

	// The head-aware selector must now prefer upstream-b exclusively.
	for range 20 {
		ups := n.pool.SelectForPathPreferCanonicalHead("/eth/v1/beacon/headers/{block_id}", 1)
		if len(ups) == 0 || ups[0].ID != "upstream-b" {
			t.Fatalf("expected head-aware selector to prefer upstream-b, got %v", ups)
		}
	}
}

// TestNetwork_Start_RunsHeadWatcherWithoutCache verifies that Network.Start
// launches the head watcher even when response caching is disabled, so the
// real-time BlockCache feed still runs on uncached networks. This exercises
// the full startup path (Network.Start) rather than calling startHeadWatcher
// directly, catching regressions where the watcher is gated on cache state.
func TestNetwork_Start_RunsHeadWatcherWithoutCache(t *testing.T) {
	t.Parallel()

	const (
		headSlot  = "300"
		headBlock = "0xfacefeed"
	)

	sseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "event: head\ndata: {\"slot\":%q,\"block\":%q}\n\n", headSlot, headBlock) //nolint:errcheck
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		case "/eth/v1/node/syncing":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"head_slot":"1","sync_distance":"0","is_syncing":false}}`))
		case "/eth/v1/beacon/states/head/finality_checkpoints":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"finalized":{"epoch":"0"}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer sseServer.Close()

	id := netID(t)
	// Long health intervals so the health monitor's periodic probe can't
	// accidentally populate BlockCache — the only path that should succeed
	// is the SSE head watcher.
	cfg := mustCfgText(t, fmt.Sprintf(`
logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: u1
        url: %q
    cache:
      enabled: false
`, id, sseServer.URL))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	if n.cache != nil {
		t.Fatal("test requires caching disabled")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.Start(ctx)
	// Stop background goroutines cleanly at end of test.
	defer func() {
		cancel()
		if n.headWatcher != nil {
			n.headWatcher.wait()
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	var seen map[string]bool
	for time.Now().Before(deadline) {
		seen = n.pool.BlockCache().CanonicalHeadSeenBy()
		if seen["u1"] {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if !seen["u1"] {
		t.Fatalf("head watcher must populate BlockCache on uncached networks, got %v", seen)
	}
}

func TestHeadWatcher_IgnoresMalformedHeadPayload(t *testing.T) {
	t.Parallel()

	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// Malformed JSON payload — parsing must fail but the watcher must
		// still fall through to cache invalidation so stale data isn't served.
		fmt.Fprint(w, "event: head\ndata: {not-json}\n\n") //nolint:errcheck
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer upstreamServer.Close()

	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`
logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: u1
        url: %q
    cache: { enabled: true, maxSize: 32 }
`, id, upstreamServer.URL))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	headKey := id + ":GET:/eth/v1/beacon/headers/head"
	n.cache.Set(headKey, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"stale":true}`), 4*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	w := startHeadWatcher(ctx, id, n.pool, n.cache, nil)
	defer func() {
		cancel()
		w.wait()
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n.cache.Get(headKey) == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if n.cache.Get(headKey) != nil {
		t.Fatal("cache entry should still be invalidated even when head payload fails to parse")
	}
	if got := n.pool.BlockCache().CanonicalHeadSeenBy(); len(got) != 0 {
		t.Fatalf("malformed payload must not populate BlockCache, got %v", got)
	}
}

func TestHeadWatcher_PrewarmsInvalidatedHeadKeys(t *testing.T) {
	t.Parallel()

	const freshBody = `{"data":{"root":"fresh-head"}}`
	var headFetches int

	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "event: head\ndata: {\"slot\":\"101\"}\n\n") //nolint:errcheck
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		case "/eth/v1/beacon/headers/head":
			headFetches++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, freshBody) //nolint:errcheck
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstreamServer.Close()

	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`
logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 2s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: u1
        url: %q
    cache: { enabled: true, maxSize: 32 }
`, id, upstreamServer.URL))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	headKey := id + ":GET:/eth/v1/beacon/headers/head"
	n.cache.Set(headKey, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"data":{"root":"stale-head"}}`), 4*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	w := startHeadWatcher(ctx, id, n.pool, n.cache, n.warmHeadCache)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entry := n.cache.Get(headKey)
		if entry != nil && string(entry.Body()) == freshBody {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	entry := n.cache.Get(headKey)
	if entry == nil {
		cancel()
		w.wait()
		t.Fatal("head cache entry was not repopulated after invalidation")
	}
	if got := string(entry.Body()); got != freshBody {
		cancel()
		w.wait()
		t.Fatalf("pre-warmed cache body = %q, want %q", got, freshBody)
	}
	if headFetches == 0 {
		cancel()
		w.wait()
		t.Fatal("expected head endpoint to be fetched during pre-warm")
	}

	cancel()
	w.wait()
}

func TestHeadWatcher_LeavesForeignNetworkKeysAlone(t *testing.T) {
	t.Parallel()

	sseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "data: {\"slot\":\"100\"}") //nolint:errcheck
	}))
	defer sseServer.Close()

	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`
logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: u1
        url: %q
    cache: { enabled: true, maxSize: 32 }
`, id, sseServer.URL))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Emulate a shared Redis DB: the scan surface contains another
	// network's head-keyed entry.
	ownKey := id + ":GET:/eth/v1/beacon/headers/head"
	foreignKey := "othernet:GET:/eth/v1/beacon/headers/head"
	hdr := http.Header{"Content-Type": []string{"application/json"}}
	n.cache.Set(ownKey, http.StatusOK, hdr, []byte(`{"own":true}`), 10*time.Second)
	n.cache.Set(foreignKey, http.StatusOK, hdr, []byte(`{"foreign":true}`), 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	w := startHeadWatcher(ctx, id, n.pool, n.cache, nil)
	defer func() {
		cancel()
		w.wait()
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n.cache.Get(ownKey) == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if n.cache.Get(ownKey) != nil {
		t.Fatal("own-network head entry was not invalidated")
	}
	if n.cache.Get(foreignKey) == nil {
		t.Fatal("foreign-network entry must not be purged by this network's head watcher")
	}
}
