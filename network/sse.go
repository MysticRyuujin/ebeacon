// Package network provides per-network request handling, including a
// fault-tolerant SSE relay that reconnects to healthy upstreams on failure.
package network

import (
	"bufio"
	"context"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mysticryuujin/ebeacon/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var metricSSEReconnects = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ebeacon_sse_reconnects_total",
	Help: "Number of SSE upstream reconnects due to failures",
}, []string{"network"})

var ssePingInterval = 15 * time.Second
var sseClientRetry = 1 * time.Second

// seenRing deduplicates SSE events by their FNV32 hash, using a circular buffer.
// When reconnecting to a new upstream we may receive events already forwarded
// to the client; the ring prevents duplicates up to its capacity.
type seenRing struct {
	buf []uint32
	pos int
}

func newSeenRing(size int) *seenRing { return &seenRing{buf: make([]uint32, size)} }

func (r *seenRing) has(h uint32) bool {
	for _, v := range r.buf {
		if v == h {
			return true
		}
	}
	return false
}

func (r *seenRing) add(h uint32) {
	r.buf[r.pos%len(r.buf)] = h
	r.pos++
}

func hashEvent(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// SSERelay handles a single client's SSE subscription.
// It maintains a persistent connection to one upstream at a time and reconnects
// automatically on failure, forwarding events without duplicates.
type SSERelay struct {
	networkID string
	pool      *upstream.Pool
}

func newSSERelay(networkID string, pool *upstream.Pool) *SSERelay {
	return &SSERelay{networkID: networkID, pool: pool}
}

func (r *SSERelay) pickUpstream(preferUpstream string, required requiredUpstreamSelector) (*upstream.Upstream, error) {
	if !required.enabled() {
		return r.pool.Get(preferUpstream)
	}
	if required.upstreamID != "" {
		u := r.pool.ByID(required.upstreamID)
		if u == nil {
			return nil, &selectedUpstreamUnavailableError{selector: required.label()}
		}
		return u, nil
	}
	if required.glob != "" {
		ups := r.pool.SelectByGlob(required.glob, 1)
		if len(ups) == 0 {
			return nil, &selectedUpstreamUnavailableError{selector: required.label()}
		}
		return ups[0], nil
	}
	ups := r.pool.SelectByClientType(required.clientType, 1)
	if len(ups) == 0 {
		return nil, &selectedUpstreamUnavailableError{selector: required.label()}
	}
	return ups[0], nil
}

// Serve streams SSE events to the client, reconnecting upstream as needed.
func (r *SSERelay) Serve(w http.ResponseWriter, req *http.Request, preferUpstream string, required requiredUpstreamSelector) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported by server", http.StatusInternalServerError)
		return
	}

	// Disable the server-level write deadline for this connection; SSE streams
	// are long-lived by design and the global WriteTimeout would otherwise kill
	// them after cfg.maxTimeout+5s.
	if rc := http.NewResponseController(w); rc != nil {
		rc.SetWriteDeadline(time.Time{}) //nolint:errcheck
	}

	ctx := req.Context()
	seen := newSeenRing(128)
	stickyID := preferUpstream
	headersSent := false

	for ctx.Err() == nil {
		u, err := r.pickUpstream(stickyID, required)
		if err != nil {
			if required.enabled() && !headersSent {
				http.Error(w, "selected upstream unavailable", http.StatusServiceUnavailable)
				return
			}
			slog.Warn("sse: no upstream available", "network", r.networkID, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		stickyID = u.ID // stay sticky on this upstream while it's healthy

		connected := r.stream(ctx, u, req, w, flusher, seen, &headersSent)
		if ctx.Err() != nil {
			return
		}

		if connected {
			// The upstream disconnected unexpectedly; try a different one.
			metricSSEReconnects.WithLabelValues(r.networkID).Inc()
			slog.Info("sse: upstream disconnected, reconnecting",
				"network", r.networkID, "upstream", u.ID)
			if !required.enabled() {
				stickyID = "" // don't prefer the failed upstream
			}
		} else if required.enabled() && !headersSent {
			http.Error(w, "selected upstream unavailable", http.StatusServiceUnavailable)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// stream opens a connection to the upstream SSE endpoint and forwards events
// until the upstream closes the stream or the client context is done.
// Returns true if the upstream connection was successfully established.
func (r *SSERelay) stream(ctx context.Context, u *upstream.Upstream, req *http.Request, w http.ResponseWriter, flusher http.Flusher, seen *seenRing, headersSent *bool) bool {
	upURL := u.URL + pathAndQueryForUpstream(req.URL)
	upReq, err := http.NewRequestWithContext(ctx, http.MethodGet, upURL, nil)
	if err != nil {
		return false
	}

	copyRequestHeaders(upReq.Header, req.Header)
	for k, v := range u.Headers {
		upReq.Header.Set(k, v)
	}

	u.IncrActive()
	resp, err := u.Client.Do(upReq)
	if err != nil {
		u.DecrActive()
		if ctx.Err() == nil {
			slog.Warn("sse: upstream connection failed", "network", r.networkID, "upstream", u.ID, "err", err)
			u.CBFailure()
		}
		return false
	}
	resp = u.TrackResponse(resp)
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		slog.Warn("sse: upstream bad status", "network", r.networkID, "upstream", u.ID, "status", resp.StatusCode)
		u.CBFailure()
		return false
	}

	if !*headersSent {
		copyResponseHeaders(w.Header(), resp.Header)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Ebeacon-Upstream", u.ID)
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("retry: " + strconv.FormatInt(sseClientRetry.Milliseconds(), 10) + "\n\n")); err != nil {
			return true
		}
		flusher.Flush()
		*headersSent = true
	}

	connectedAt := time.Now()
	slog.Debug("sse: upstream connected", "network", r.networkID, "upstream", u.ID)

	// Read line-by-line without Scanner's fixed token limit; beacon events may
	// include large payloads for some topics.
	reader := bufio.NewReader(resp.Body)
	var event strings.Builder
	type readResult struct {
		line string
		err  error
	}
	readCh := make(chan readResult, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			line, readErr := reader.ReadString('\n')
			select {
			case readCh <- readResult{line: line, err: readErr}:
			case <-done:
				return
			}
			if readErr != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(ssePingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return true
		case <-pingTicker.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return true
			}
			flusher.Flush()
		case rr := <-readCh:
			if rr.line != "" {
				event.WriteString(rr.line)
				if rr.line == "\n" || rr.line == "\r\n" {
					if !flushSSEEvent(w, flusher, seen, &event, false) {
						return true
					}
				}
			}

			if rr.err == nil {
				continue
			}
			if rr.err == io.EOF {
				if !flushSSEEvent(w, flusher, seen, &event, true) {
					return true
				}
				u.CBSuccess()
				slog.Info("sse: upstream closed connection",
					"network", r.networkID, "upstream", u.ID,
					"duration", time.Since(connectedAt).Round(time.Second))
				return true
			}
			if ctx.Err() == nil {
				slog.Warn("sse: read error", "network", r.networkID, "upstream", u.ID,
					"err", rr.err, "duration", time.Since(connectedAt).Round(time.Second))
			}
			u.CBFailure()
			return true
		}
	}
}

func flushSSEEvent(w http.ResponseWriter, flusher http.Flusher, seen *seenRing, event *strings.Builder, forceTerminator bool) bool {
	if event.Len() == 0 {
		return true
	}

	evStr := event.String()
	if forceTerminator {
		evStr = strings.TrimRight(evStr, "\r\n") + "\n\n"
	}

	h := hashEvent(evStr)
	if seen.has(h) {
		event.Reset()
		return true
	}
	seen.add(h)

	if _, err := w.Write([]byte(evStr)); err != nil {
		return false
	}
	flusher.Flush()
	event.Reset()
	return true
}
