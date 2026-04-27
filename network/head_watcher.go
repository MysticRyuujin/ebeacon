package network

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mysticryuujin/ebeacon/cache"
	"github.com/mysticryuujin/ebeacon/upstream"
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

	metricReorgInvalidations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_reorg_cache_invalidations_total",
		Help: "Cache entries invalidated on chain_reorg event",
	}, []string{"network"})

	metricFinalityPromotions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_finality_cache_promotions_total",
		Help: "Cache entries promoted to permanent on finalized_checkpoint event",
	}, []string{"network"})
)

// headWatcher maintains one best-effort SSE subscription per upstream to the
// head event topic. When a new head is announced, it records the block into
// the pool's BlockCache (so routing can hard-prefer upstreams that have seen
// the new head) and purges cached responses for named-slot-ID paths (head,
// finalized, justified) so stale data is not served across a slot boundary.
//
// Behaviour on failure: each per-upstream watcher reconnects with exponential
// backoff and is independent of the others. The request path is never blocked
// by watcher state.
type headWatcher struct {
	networkID string
	pool      *upstream.Pool
	cache     *cache.Cache
	warm      func(context.Context, *upstream.Upstream, []string)
	done      chan struct{}
	wg        sync.WaitGroup

	// cancelWarm cancels the previous warm goroutine so stale pre-warm
	// requests don't race with a newer invalidation cycle. Shared across all
	// per-upstream watchers so only one pre-warm is in flight at a time.
	warmMu     sync.Mutex
	cancelWarm context.CancelFunc
}

// startHeadWatcher starts the background watcher goroutines, one per upstream.
// Exits when ctx is cancelled.
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

// wait blocks until all watcher goroutines have exited.
func (w *headWatcher) wait() { <-w.done }

func (w *headWatcher) run(ctx context.Context) {
	defer close(w.done)

	// Snapshot the upstream set once at startup. The pool does not currently
	// support dynamic add/remove of upstreams after construction, so a single
	// snapshot is sufficient.
	upstreams := w.pool.All()
	for _, u := range upstreams {
		w.wg.Add(1)
		go w.watchUpstream(ctx, u)
	}
	w.wg.Wait()
}

// watchUpstream maintains a single long-lived SSE subscription to u, with
// exponential backoff on failure.
func (w *headWatcher) watchUpstream(ctx context.Context, u *upstream.Upstream) {
	defer w.wg.Done()

	const (
		minBackoff      = 1 * time.Second
		maxBackoff      = 60 * time.Second
		stableThreshold = 30 * time.Second // connection was healthy; reset backoff
	)

	backoff := minBackoff

	for {
		if ctx.Err() != nil {
			return
		}

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

// subscribe opens a streaming GET to /eth/v1/events on u for head,
// finalized_checkpoint, and chain_reorg topics, and processes lines until the
// connection closes, ctx is cancelled, or no data arrives for 90 seconds
// (indicating a silent upstream or network stall).
func (w *headWatcher) subscribe(ctx context.Context, u *upstream.Upstream) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		u.URL+"/eth/v1/events?topics=head,finalized_checkpoint,chain_reorg", nil)
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
	inFinalizedEvent := false
	inReorgEvent := false
	sawEventName := false
	sawData := false
	var dataPayload strings.Builder

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
					eventName := strings.TrimSpace(strings.TrimPrefix(line, "event:"))
					inHeadEvent = strings.EqualFold(eventName, "head")
					inFinalizedEvent = strings.EqualFold(eventName, "finalized_checkpoint")
					inReorgEvent = strings.EqualFold(eventName, "chain_reorg")
				case strings.HasPrefix(line, "data:"):
					if inHeadEvent || inFinalizedEvent || inReorgEvent || !sawEventName {
						sawData = true
						dataPayload.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
					}
				case line == "":
					if sawData {
						w.dispatchEvent(ctx, u, inHeadEvent, inFinalizedEvent, inReorgEvent, sawEventName, dataPayload.String())
					}
					inHeadEvent = false
					inFinalizedEvent = false
					inReorgEvent = false
					sawEventName = false
					sawData = false
					dataPayload.Reset()
				}
			}

			if rr.err != nil {
				if sawData {
					w.dispatchEvent(ctx, u, inHeadEvent, inFinalizedEvent, inReorgEvent, sawEventName, dataPayload.String())
				}
				if rr.err == io.EOF || ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("upstream %s: %w", u.ID, rr.err)
			}
		}
	}
}

// dispatchEvent routes a completed SSE event to the appropriate handler.
func (w *headWatcher) dispatchEvent(ctx context.Context, u *upstream.Upstream, isHead, isFinalized, isReorg, sawEventName bool, data string) {
	switch {
	case isHead || !sawEventName:
		// Record the block in the BlockCache first so routing decisions
		// made inside the pre-warm request can already see this upstream
		// as a canonical-head reporter.
		w.recordHeadSeen(u, data)
		w.invalidateHeadCache(ctx, u)
	case isFinalized:
		w.handleFinalizedCheckpoint(data)
	case isReorg:
		w.handleChainReorg(data)
	}
}

// recordHeadSeen parses the JSON payload of a head SSE event and records the
// block into the pool's BlockCache. This lets the router hard-prefer this
// upstream for subsequent /head, /finalized, /justified requests — closing the
// race where a client receives the event and immediately queries the block
// before slower upstreams have caught up.
//
// Head event payload shape (Beacon API spec):
//
//	{"slot":"123","block":"0xabc...","state":"...","epoch_transition":false,...}
//
// Malformed or empty payloads are silently ignored; the cache-invalidation
// path still runs so cache correctness is preserved even if parsing fails.
func (w *headWatcher) recordHeadSeen(u *upstream.Upstream, data string) {
	if data == "" {
		return
	}
	var payload struct {
		Slot  string `json:"slot"`
		Block string `json:"block"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		slog.Debug("head watcher: failed to parse head data",
			"network", w.networkID, "upstream", u.ID, "err", err)
		return
	}
	slot, err := strconv.ParseUint(payload.Slot, 10, 64)
	if err != nil || slot == 0 || payload.Block == "" {
		return
	}
	u.UpdateHeadBlock(slot, payload.Block, "")
	w.pool.BlockCache().AddBlock(u.ID, slot, payload.Block, "")
	w.pool.SyncCanonicalHead()
}

func (w *headWatcher) invalidateHeadCache(ctx context.Context, u *upstream.Upstream) {
	// No-op on networks with caching disabled; recordHeadSeen still runs
	// unconditionally so BlockCache-based routing keeps working.
	if w.cache == nil {
		return
	}
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

// handleFinalizedCheckpoint processes a finalized_checkpoint SSE event.
// It updates the pool's finalized epoch and promotes existing cache entries
// for slots/epochs that are now finalized.
func (w *headWatcher) handleFinalizedCheckpoint(data string) {
	var payload struct {
		Epoch string `json:"epoch"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		slog.Debug("head watcher: failed to parse finalized_checkpoint data",
			"network", w.networkID, "err", err)
		return
	}
	epoch, err := strconv.ParseUint(payload.Epoch, 10, 64)
	if err != nil || epoch == 0 {
		return
	}

	// Pool epoch tracking is independent of caching and must always run.
	w.pool.UpdateFinalizedEpoch(epoch)

	// Cache promotion only applies when caching is enabled.
	if w.cache == nil {
		return
	}

	finalizedSlot := epoch*32 + 31
	n := w.cache.PromoteIf(func(key string) bool {
		path := cacheKeyPath(key)
		if path == "" {
			return false
		}
		if slot, ok := pathNumericSlot(path); ok && slot <= finalizedSlot {
			return true
		}
		if ep, ok := pathNumericEpoch(path); ok && ep <= epoch {
			return true
		}
		return false
	})
	if n > 0 {
		metricFinalityPromotions.WithLabelValues(w.networkID).Add(float64(n))
		slog.Debug("head watcher: promoted cache entries on finality",
			"network", w.networkID, "epoch", epoch, "count", n)
	}
}

// handleChainReorg processes a chain_reorg SSE event by purging cached
// responses for numeric slots at or above the reorg slot. These entries
// may contain data from the now-orphaned fork.
func (w *headWatcher) handleChainReorg(data string) {
	if w.cache == nil {
		return
	}
	var payload struct {
		Slot string `json:"slot"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		slog.Debug("head watcher: failed to parse chain_reorg data",
			"network", w.networkID, "err", err)
		return
	}
	reorgSlot, err := strconv.ParseUint(payload.Slot, 10, 64)
	if err != nil || reorgSlot == 0 {
		return
	}

	// Purge numeric-slot entries at or above the reorg slot, plus all
	// named-head entries which may also reference the orphaned fork.
	n := w.cache.PurgeIf(func(key string) bool {
		path := cacheKeyPath(key)
		if path == "" {
			return false
		}
		if pathHasNamedSlotID(path) {
			return true
		}
		if slot, ok := pathNumericSlot(path); ok && slot >= reorgSlot {
			return true
		}
		return false
	})
	if n > 0 {
		metricReorgInvalidations.WithLabelValues(w.networkID).Add(float64(n))
		slog.Info("head watcher: purged cache entries on chain reorg",
			"network", w.networkID, "slot", reorgSlot, "count", n)
	}
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
