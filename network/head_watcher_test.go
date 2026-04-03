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
