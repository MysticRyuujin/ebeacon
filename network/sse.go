// Package network provides per-network request handling, including a
// fault-tolerant SSE relay that reconnects to healthy upstreams on failure.
package network

import (
	"bufio"
	"context"
	"errors"
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

// sseIdleTimeout bounds upstream silence before the relay reconnects. Without
// it a half-dead upstream TCP session blocks the read loop forever while the
// relay's own pings keep the client connection looking alive. Var for tests.
var sseIdleTimeout = 90 * time.Second

// sseMaxEventBytes caps per-event accumulation so a malicious or broken
// upstream cannot grow relay memory without bound.
const sseMaxEventBytes = 4 << 20

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
		} else if required.enabled() && !headersSent {
			http.Error(w, "selected upstream unavailable", http.StatusServiceUnavailable)
			return
		}
		if !required.enabled() {
			stickyID = "" // don't prefer the failed upstream
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

	tok, ok := u.CBTryAcquire()
	if !ok {
		return false
	}
	defer tok.Release()
	u.ConsumeRateToken()
	u.IncrActive()
	resp, err := u.Client.Do(upReq)
	if err != nil {
		err = upstream.SanitizeError(err)
		u.DecrActive()
		if ctx.Err() == nil {
			slog.Warn("sse: upstream connection failed", "network", r.networkID, "upstream", u.ID, "err", err)
			tok.Failure()
		}
		return false
	}
	resp = u.TrackResponse(resp)
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		slog.Warn("sse: upstream bad status", "network", r.networkID, "upstream", u.ID, "status", resp.StatusCode)
		tok.Failure()
		return false
	}

	if !*headersSent {
		copyResponseHeaders(w.Header(), resp.Header)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Ebeacon-Upstream", ObfuscateUpstreamID(u.ID))
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("retry: " + strconv.FormatInt(sseClientRetry.Milliseconds(), 10) + "\n\n")); err != nil {
			return true
		}
		flusher.Flush()
		*headersSent = true
	}

	connectedAt := time.Now()
	slog.Debug("sse: upstream connected", "network", r.networkID, "upstream", u.ID)

	// Success is credited only once a real event reaches the client: a frame
	// carrying a non-comment field, newly delivered rather than deduplicated.
	// Crediting anything cheaper (the connect, a keep-alive, a replay of an
	// event already seen) would zero the failure count on every reconnect, so
	// an upstream that serves nothing new could never trip the breaker.
	credited := false
	eventHasField := false

	// Read line-by-line without Scanner's fixed token limit; beacon events may
	// include large payloads for some topics. Lines are capped so a stream
	// that never sends a newline can't grow memory unbounded before the
	// per-event size check downstream ever runs.
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
			line, readErr := readCappedLine(reader, sseMaxEventBytes)
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

	idle := time.NewTimer(sseIdleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-ctx.Done():
			return true
		case <-idle.C:
			// No CBFailure: a stream subscribed to rare topics is
			// legitimately silent; a reconnect is cheap, an opened breaker
			// is not.
			slog.Warn("sse: upstream idle, reconnecting",
				"network", r.networkID, "upstream", u.ID,
				"idle", sseIdleTimeout, "duration", time.Since(connectedAt).Round(time.Second))
			return true
		case <-pingTicker.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return true
			}
			flusher.Flush()
		case rr := <-readCh:
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(sseIdleTimeout)

			if rr.line != "" {
				if event.Len()+len(rr.line) > sseMaxEventBytes {
					// Reconnect rather than skip: emitting a truncated
					// event would hand the client corrupt data.
					slog.Warn("sse: event exceeds size cap, reconnecting",
						"network", r.networkID, "upstream", u.ID, "size", event.Len())
					return true
				}
				terminator := rr.line == "\n" || rr.line == "\r\n"
				if !terminator && !strings.HasPrefix(rr.line, ":") {
					eventHasField = true
				}
				event.WriteString(rr.line)
				if terminator {
					wrote, ok := flushSSEEvent(w, flusher, seen, &event, false)
					if !ok {
						return true
					}
					if wrote && eventHasField && !credited {
						tok.Success()
						credited = true
					}
					eventHasField = false
				}
			}

			if rr.err == nil {
				continue
			}
			if rr.err == io.EOF {
				wrote, ok := flushSSEEvent(w, flusher, seen, &event, true)
				if !ok {
					return true
				}
				if wrote && eventHasField && !credited {
					tok.Success()
					credited = true
				}
				// Closing cleanly having served nothing is still a fault.
				if !credited {
					tok.Failure()
				}
				slog.Info("sse: upstream closed connection",
					"network", r.networkID, "upstream", u.ID,
					"duration", time.Since(connectedAt).Round(time.Second))
				return true
			}
			if ctx.Err() == nil {
				slog.Warn("sse: read error", "network", r.networkID, "upstream", u.ID,
					"err", rr.err, "duration", time.Since(connectedAt).Round(time.Second))
				// A stream that already credited its success no longer owns a
				// recovery probe, so the drop counts against closed-state
				// accounting only.
				if credited {
					u.CBFailure()
				} else {
					tok.Failure()
				}
			}
			return true
		}
	}
}

// errSSELineTooLong signals that a single line exceeded the cap without a
// newline — an abusive or broken upstream. The reader stops so memory can't
// grow past the cap plus one bufio buffer.
var errSSELineTooLong = errors.New("sse: line exceeds size cap")

// readCappedLine reads up to and including the next '\n', or until max bytes
// have accumulated without one, in which case it returns the partial line and
// errSSELineTooLong instead of buffering the rest of a newline-free stream.
func readCappedLine(r *bufio.Reader, max int) (string, error) {
	var sb strings.Builder
	for {
		chunk, err := r.ReadSlice('\n')
		sb.Write(chunk)
		if err == bufio.ErrBufferFull {
			if sb.Len() > max {
				return sb.String(), errSSELineTooLong
			}
			continue
		}
		return sb.String(), err
	}
}

// flushSSEEvent reports whether it wrote the event to the client, and whether
// the client is still usable. An empty or already-seen event yields
// (false, true): nothing reached the client, but that is not an error.
func flushSSEEvent(w http.ResponseWriter, flusher http.Flusher, seen *seenRing, event *strings.Builder, forceTerminator bool) (wrote, ok bool) {
	if event.Len() == 0 {
		return false, true
	}

	evStr := event.String()
	if forceTerminator {
		evStr = strings.TrimRight(evStr, "\r\n") + "\n\n"
	}

	h := hashEvent(evStr)
	if seen.has(h) {
		event.Reset()
		return false, true
	}
	seen.add(h)

	if _, err := w.Write([]byte(evStr)); err != nil {
		return false, false
	}
	flusher.Flush()
	event.Reset()
	return true, true
}
