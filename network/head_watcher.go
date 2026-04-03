package network

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ebeacon/ebeacon/cache"
	"github.com/ebeacon/ebeacon/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricHeadInvalidations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_head_cache_invalidations_total",
		Help: "Cache entries invalidated on head event from the beacon node SSE stream",
	}, []string{"network"})

	metricHeadWatcherReconnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_head_watcher_reconnects_total",
		Help: "Number of head event watcher upstream reconnects",
	}, []string{"network"})
)

// headWatcher maintains a best-effort SSE subscription to one upstream's head
// event topic. When a new head is announced, it purges cached responses for
// named-slot-ID paths (head, finalized, justified) so stale data is not served
// across a slot boundary.
//
// Behaviour on failure: the watcher reconnects with exponential backoff, trying
// a different upstream on each attempt. If no upstream is available the watcher
// sleeps and retries. The request path is never blocked by watcher state.
type headWatcher struct {
	networkID string
	pool      *upstream.Pool
	cache     *cache.Cache
	warm      func(context.Context, *upstream.Upstream, []string)
	done      chan struct{}

	// cancelWarm cancels the previous warm goroutine so stale pre-warm
	// requests don't race with a newer invalidation cycle.
	warmMu     sync.Mutex
	cancelWarm context.CancelFunc
}

// startHeadWatcher starts the background watcher goroutine. It exits when ctx
// is cancelled.
func startHeadWatcher(ctx context.Context, networkID string, pool *upstream.Pool, c *cache.Cache, warm func(context.Context, *upstream.Upstream, []string)) *headWatcher {
	w := &headWatcher{
		networkID: networkID,
		pool:      pool,
		cache:     c,
		warm:      warm,
		done:      make(chan struct{}),
	}
	go w.run(ctx)
	return w
}

// wait blocks until the watcher goroutine has exited.
func (w *headWatcher) wait() { <-w.done }

func (w *headWatcher) run(ctx context.Context) {
	defer close(w.done)

	const (
		minBackoff      = 1 * time.Second
		maxBackoff      = 60 * time.Second
		stableThreshold = 30 * time.Second // connection was healthy; reset backoff
	)

	backoff := minBackoff
	upstreamIdx := 0

	for {
		if ctx.Err() != nil {
			return
		}

		candidates := w.pool.All()
		if len(candidates) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			}
			continue
		}

		u := candidates[upstreamIdx%len(candidates)]
		upstreamIdx++

		start := time.Now()
		err := w.subscribe(ctx, u)

		if ctx.Err() != nil {
			return // clean shutdown; don't log or reconnect
		}

		if time.Since(start) >= stableThreshold {
			backoff = minBackoff // connection was stable; reset backoff
		} else {
			backoff = min(backoff*2, maxBackoff)
		}

		if err != nil {
			slog.Debug("head watcher: subscription ended",
				"network", w.networkID, "upstream", u.ID,
				"err", err, "reconnect_in", backoff)
			metricHeadWatcherReconnects.WithLabelValues(w.networkID).Inc()
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// subscribe opens a streaming GET to /eth/v1/events?topics=head on u and
// processes lines until the connection closes, ctx is cancelled, or no data
// arrives for 90 seconds (indicating a silent upstream or network stall).
func (w *headWatcher) subscribe(ctx context.Context, u *upstream.Upstream) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		u.URL+"/eth/v1/events?topics=head", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	for k, v := range u.Headers {
		req.Header.Set(k, v)
	}

	resp, err := u.Client.Do(req)
	if err != nil {
		return err
	}
	// readerDone is closed first (LIFO defer order) so the reader goroutine can
	// exit via its select before resp.Body.Close() interrupts the read.
	readerDone := make(chan struct{})
	defer resp.Body.Close() //nolint:errcheck
	defer close(readerDone)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upstream %s: status %d", u.ID, resp.StatusCode)
	}

	slog.Debug("head watcher: connected", "network", w.networkID, "upstream", u.ID)

	// Spawn a goroutine to do blocking line reads so the outer loop can also
	// select on ctx.Done() and a stale-connection timeout.
	type lineResult struct {
		line string
		err  error
	}
	lines := make(chan lineResult, 4)
	go func() {
		r := bufio.NewReader(resp.Body)
		for {
			line, err := r.ReadString('\n')
			select {
			case lines <- lineResult{line, err}:
			case <-readerDone:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	// Beacon nodes emit a head event roughly every slot (12 s). If we receive
	// nothing for 90 s the connection is likely stalled.
	const staleTimeout = 90 * time.Second
	idle := time.NewTimer(staleTimeout)
	defer idle.Stop()

	inHeadEvent := false
	sawEventName := false
	sawHeadData := false

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-idle.C:
			return fmt.Errorf("upstream %s: no data for %s", u.ID, staleTimeout)

		case rr := <-lines:
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(staleTimeout)

			if rr.line != "" {
				line := strings.TrimRight(rr.line, "\r\n")

				switch {
				case strings.HasPrefix(line, "event:"):
					sawEventName = true
					inHeadEvent = strings.EqualFold(strings.TrimSpace(strings.TrimPrefix(line, "event:")), "head")
				case strings.HasPrefix(line, "data:"):
					if inHeadEvent || !sawEventName {
						sawHeadData = true
					}
				case line == "":
					if sawHeadData {
						w.invalidateHeadCache(ctx, u)
					}
					inHeadEvent = false
					sawEventName = false
					sawHeadData = false
				}
			}

			if rr.err != nil {
				if sawHeadData {
					w.invalidateHeadCache(ctx, u)
				}
				if rr.err == io.EOF || ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("upstream %s: %w", u.ID, rr.err)
			}
		}
	}
}

func (w *headWatcher) invalidateHeadCache(ctx context.Context, u *upstream.Upstream) {
	// Single-pass: purge matching entries and collect their keys for warming.
	n, keys := w.cache.PurgeCollect(isNamedHeadCacheKey)
	if n == 0 {
		return
	}

	metricHeadInvalidations.WithLabelValues(w.networkID).Add(float64(n))
	slog.Debug("head watcher: invalidated head cache entries",
		"network", w.networkID, "upstream", u.ID, "count", n)

	if w.warm != nil {
		// Cancel any in-flight warm from a previous head event so we don't
		// waste upstream requests on data that is already stale again.
		w.warmMu.Lock()
		if w.cancelWarm != nil {
			w.cancelWarm()
		}
		warmCtx, cancel := context.WithCancel(ctx)
		w.cancelWarm = cancel
		w.warmMu.Unlock()

		go w.warm(warmCtx, u, keys)
	}
}

// isNamedHeadCacheKey reports whether key corresponds to a request that used a
// named slot identifier (head, finalized, justified).
func isNamedHeadCacheKey(key string) bool {
	return pathHasNamedSlotID(cacheKeyPath(key))
}

// cacheKeyPath extracts the URL path component from a cache key.
//
// Key format (from buildCacheKey):
//
//	networkID:METHOD:path[?query][:accept=binary][:upstream=scope]
func cacheKeyPath(key string) string {
	parsed, ok := parseCacheKey(key)
	if !ok {
		return ""
	}
	return parsed.path
}
