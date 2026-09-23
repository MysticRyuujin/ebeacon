package network

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mysticryuujin/ebeacon/cache"
	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/debuglog"
	"github.com/mysticryuujin/ebeacon/reqctx"
	"github.com/mysticryuujin/ebeacon/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

// statusClientClosedRequest is nginx's non-standard 499, used only in metrics
// to distinguish client disconnects from real error responses.
const statusClientClosedRequest = 499

// hop-by-hop headers that must not be forwarded.
// The first group is the standard set defined in RFC 7230 §6.1.
// The second group is ebeacon-specific: these headers carry client auth tokens
// and upstream-selection hints that are only meaningful to ebeacon itself and
// must never reach the backend beacon nodes.
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Proxy-Connection", "Te", "Trailer", "Trailers", "Transfer-Encoding", "Upgrade",
	"X-EBEACON-Use-Upstream",
	"X-EBEACON-Secret-Token",
	"X-API-Key",
	"Authorization",
}

var hopByHopHeaderSet = func() map[string]struct{} {
	out := make(map[string]struct{}, len(hopByHopHeaders))
	for _, h := range hopByHopHeaders {
		out[strings.ToLower(h)] = struct{}{}
	}
	return out
}()

// pathNumericSlot extracts a numeric slot/state/block identifier from known
// Beacon API routes whose numeric path component represents a slot.
func pathNumericSlot(path string) (uint64, bool) {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) < 5 || segments[0] != "eth" || len(segments[1]) < 2 || segments[1][0] != 'v' || segments[2] != "beacon" {
		return 0, false
	}

	parse := func(seg string) (uint64, bool) {
		if seg == "" || len(seg) > 12 || seg[0] < '0' || seg[0] > '9' {
			return 0, false
		}
		n, err := strconv.ParseUint(seg, 10, 64)
		if err != nil || n == 0 {
			return 0, false
		}
		return n, true
	}

	switch segments[3] {
	case "headers", "blocks", "blinded_blocks", "blobs", "blob_sidecars", "states":
		return parse(segments[4])
	case "rewards":
		if len(segments) < 6 {
			return 0, false
		}
		switch segments[4] {
		case "blocks", "sync_committee":
			return parse(segments[5])
		}
	}

	return 0, false
}

// namedSlotIDs is the set of non-numeric Beacon API block/state identifiers
// that refer to a chain-tip-relative position. Their cached responses become
// stale at the next slot boundary, so they benefit from slot-aligned TTLs.
var namedSlotIDs = map[string]bool{
	"head":      true,
	"finalized": true,
	"justified": true,
}

// pathHasNamedSlotID reports whether the path's block/state ID component is a
// named identifier (head, finalized, justified) rather than a numeric slot or
// state root. Bare /eth/v1/beacon/headers (no block_id) also returns true
// because the omitted block_id defaults to head.
// isFinalizedPath reports whether path addresses data that the finalized
// checkpoint at finalizedEpoch has fixed. A checkpoint at epoch E finalizes
// slots up to E*slotsPerEpoch. Epoch-keyed data (e.g. attestation rewards
// for epoch N) can depend on inclusions through epoch N+1, so it requires
// N+2 <= E, written as N <= E-2 to avoid uint64 wrap on a huge epoch.
func isFinalizedPath(path string, finalizedEpoch, slotsPerEpoch uint64) bool {
	if finalizedEpoch == 0 {
		return false
	}
	if slot, ok := pathNumericSlot(path); ok && slot <= finalizedEpoch*slotsPerEpoch {
		return true
	}
	epoch, ok := pathNumericEpoch(path)
	return ok && finalizedEpoch >= 2 && epoch <= finalizedEpoch-2
}

func pathHasNamedSlotID(path string) bool {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) < 4 || segments[0] != "eth" || len(segments[1]) < 2 ||
		segments[1][0] != 'v' || segments[2] != "beacon" {
		return false
	}
	switch segments[3] {
	case "headers":
		if len(segments) < 5 {
			return true // bare /headers implies head
		}
		return namedSlotIDs[segments[4]]
	case "blocks", "blinded_blocks", "blobs", "blob_sidecars", "states":
		if len(segments) < 5 {
			return false
		}
		return namedSlotIDs[segments[4]]
	}
	return false
}

// pathNumericEpoch extracts an epoch parameter from cacheable epoch-scoped
// endpoints whose responses become immutable after finalization.
func pathNumericEpoch(path string) (uint64, bool) {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) != 6 {
		return 0, false
	}
	if segments[0] != "eth" || len(segments[1]) < 2 || segments[1][0] != 'v' {
		return 0, false
	}
	switch {
	case segments[2] == "beacon" && segments[3] == "rewards" && segments[4] == "attestations":
	case segments[2] == "validator" && segments[3] == "duties" && segments[4] == "proposer":
	default:
		return 0, false
	}
	epoch, err := strconv.ParseUint(segments[5], 10, 64)
	if err != nil {
		return 0, false
	}
	return epoch, true
}

// gzipPool reuses gzip.Writer objects to avoid the ~32 KB per-call allocation
// that gzip.NewWriter incurs when compressing responses to clients.
var gzipPool = sync.Pool{
	New: func() interface{} {
		gz, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		return gz
	},
}
var noisyCacheQueryPathRe = regexp.MustCompile(`^/eth/v\d+/node/(?:peer_count|syncing)$`)

var peersCacheQueryPathRe = regexp.MustCompile(`^/eth/v\d+/node/peers$`)

// Network handles all proxying for a single beacon chain network.
type Network struct {
	id       string
	cfg      *config.NetworkConfig
	failsafe config.FailsafeConfig
	rl       config.RateLimitingConfig
	pool     *upstream.Pool
	cache    *cache.Cache // nil if caching disabled
	relay    *SSERelay
	sessions *SessionManager  // nil if rate-limiting + stickiness both disabled
	routing  *compiledRouting // nil if no clientRoutes / routeRules

	blocked []*regexp.Regexp

	// Global rate limiter (shared across all IPs)
	globalLimiter *rate.Limiter

	// Request deduplication
	inflight singleflight.Group

	// Per-method failsafe overrides
	failsafeOverrides []compiledFailsafeOverride

	// Consensus policy
	consensus *ConsensusPolicy

	// Head event watcher. Runs on all networks (including cache-disabled ones)
	// because it feeds BlockCache for head-aware routing in addition to
	// invalidating cached responses.
	headWatcher *headWatcher

	// gzip enabled
	gzipEnabled bool

	// maxResponseBytes caps buffered upstream response bodies (post-gzip).
	maxResponseBytes int64

	// maxTimeout bounds detached multiplexed executions (server.maxTimeout).
	maxTimeout time.Duration

	reqTotal            *prometheus.CounterVec
	reqByMethod         *prometheus.CounterVec
	reqByPath           *prometheus.CounterVec
	reqByAPIKey         *prometheus.CounterVec
	reqByAPIKeyPath     *prometheus.CounterVec
	cacheByMethod       *prometheus.CounterVec
	cacheByPath         *prometheus.CounterVec
	reqDuration         prometheus.ObserverVec
	reqDurationByMethod prometheus.ObserverVec
	reqDurationByPath   prometheus.ObserverVec
	cacheServed         prometheus.Counter
	multiplexedTotal    prometheus.Counter
}

type compiledFailsafeOverride struct {
	pathMethodRule
	failsafe config.FailsafeConfig
}

type selectedUpstreamUnavailableError struct {
	selector string
	err      error
}

func (e *selectedUpstreamUnavailableError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("selected upstream %q unavailable: %v", e.selector, e.err)
	}
	return fmt.Sprintf("selected upstream %q unavailable", e.selector)
}

func (e *selectedUpstreamUnavailableError) Unwrap() error {
	return e.err
}

// New constructs a Network from config. Call Start to begin health monitoring.
func New(cfg *config.NetworkConfig, globalCfg *config.Config) (*Network, error) {
	fs := globalCfg.EffectiveFailsafe(cfg)
	rl := globalCfg.EffectiveRateLimiting(cfg)
	health := globalCfg.EffectiveHealth(cfg)

	pool, err := upstream.NewPool(
		cfg.ID,
		cfg.Upstreams,
		cfg.Routing,
		health,
		fs.CircuitBreaker,
	)
	if err != nil {
		return nil, fmt.Errorf("network %s: %w", cfg.ID, err)
	}
	pool.SetChainTiming(cfg.GenesisTime, cfg.SecondsPerSlot, uint64(cfg.SlotsPerEpoch))

	n := &Network{
		id:               cfg.ID,
		cfg:              cfg,
		failsafe:         fs,
		rl:               rl,
		pool:             pool,
		relay:            newSSERelay(cfg.ID, pool),
		gzipEnabled:      globalCfg.Server.GzipEnabled(),
		maxResponseBytes: globalCfg.Server.MaxResponseBodyBytes,
		maxTimeout:       globalCfg.Server.MaxTimeout,
	}

	cr, err := compileRouting(&cfg.Routing)
	if err != nil {
		return nil, fmt.Errorf("network %s: routing: %w", cfg.ID, err)
	}
	n.routing = cr

	// Compile blocked path patterns.
	for _, pat := range cfg.Routing.BlockedPaths {
		re, err := regexp.Compile(pat)
		if err != nil {
			slog.Warn("invalid blocked path pattern", "network", cfg.ID, "pattern", pat)
			continue
		}
		n.blocked = append(n.blocked, re)
	}

	// Build cache if enabled.
	if cfg.Cache.Enabled {
		c, err := cache.New(cfg.ID, cfg.Cache)
		if err != nil {
			return nil, fmt.Errorf("network %s: cache: %w", cfg.ID, err)
		}
		n.cache = c
	}

	// Build session manager if rate-limiting or stickiness is needed.
	if rl.PerIP != nil || cfg.Routing.StickySession {
		var limit float64
		var burst int
		if rl.PerIP != nil {
			limit = rl.PerIP.Limit
			burst = rl.PerIP.Burst
		}
		n.sessions = newSessionManager(limit, burst, cfg.Routing.StickyTimeout)
	}

	// Global rate limiter
	if rl.Global != nil {
		n.globalLimiter = rate.NewLimiter(rate.Limit(rl.Global.Limit), rl.Global.Burst)
	}

	// Per-method failsafe overrides
	for _, fo := range cfg.FailsafeOverrides {
		rule, err := compilePathMethodRule(fo.PathPattern, fo.Methods)
		if err != nil {
			slog.Warn("invalid failsafe override pattern", "network", cfg.ID, "pattern", fo.PathPattern)
			continue
		}
		n.failsafeOverrides = append(n.failsafeOverrides, compiledFailsafeOverride{
			pathMethodRule: rule, failsafe: mergeFailsafe(n.failsafe, fo.Failsafe),
		})
	}

	// Consensus policy
	n.consensus = NewConsensusPolicy(fs.Consensus)
	if n.consensus != nil {
		n.consensus.MaxBodyBytes = n.maxResponseBytes
	}

	// Prometheus metrics (per-network labels).
	netLabel := prometheus.Labels{"network": cfg.ID}
	n.reqTotal = metricReqTotal.MustCurryWith(netLabel)
	n.reqDuration = metricReqDuration.MustCurryWith(netLabel)
	n.reqDurationByMethod = metricReqDurationByMethod.MustCurryWith(netLabel)
	n.reqDurationByPath = metricReqDurationByPath.MustCurryWith(netLabel)
	n.reqByMethod = metricReqByMethod.MustCurryWith(netLabel)
	n.reqByPath = metricReqByPath.MustCurryWith(netLabel)
	n.reqByAPIKey = metricReqByAPIKey.MustCurryWith(netLabel)
	n.reqByAPIKeyPath = metricReqByAPIKeyPath.MustCurryWith(netLabel)
	n.cacheByMethod = metricCacheByMethod.MustCurryWith(netLabel)
	n.cacheByPath = metricCacheByPath.MustCurryWith(netLabel)
	n.cacheServed = metricCacheServed.WithLabelValues(cfg.ID)
	n.multiplexedTotal = metricMultiplexedTotal.WithLabelValues(cfg.ID)

	return n, nil
}

// Pool returns the upstream pool (used by API handlers).
func (n *Network) Pool() *upstream.Pool { return n.pool }

// ID returns the network identifier.
func (n *Network) ID() string { return n.id }

// HealthStatus returns the best health status across all upstreams in this network.
func (n *Network) HealthStatus() upstream.HealthStatus {
	return n.pool.NodeHealthStatus(upstream.Selector{})
}

func (n *Network) serveHealthz(w http.ResponseWriter, clientUpstream string) {
	sel := upstream.ParseSelector(clientUpstream)
	total, up, degraded, down := n.pool.HealthCounts(sel)
	status, code := HealthzStatus(n.pool.NodeHealthStatus(sel))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"status":%q,"upstreams":%d,"healthy":%d,"degraded":%d,"down":%d}`, //nolint:errcheck
		status, total, up, degraded, down)
}

// HealthzStatus maps a health status to the /healthz status label and HTTP code.
func HealthzStatus(hs upstream.HealthStatus) (string, int) {
	switch hs {
	case upstream.HealthUp:
		return "ok", http.StatusOK
	case upstream.HealthDegraded:
		return "degraded", http.StatusOK
	default:
		return "down", http.StatusServiceUnavailable
	}
}

// Cache returns the cache instance (may be nil).
func (n *Network) CacheInstance() *cache.Cache { return n.cache }

// Sessions returns the session manager (may be nil).
func (n *Network) Sessions() *SessionManager { return n.sessions }

// Start launches background health monitoring and session rebalancing.
func (n *Network) Start(ctx context.Context) {
	n.pool.Start(ctx)
	if n.sessions != nil {
		n.sessions.StartCleanup(ctx)
		if n.cfg.Routing.StickySession {
			n.sessions.StartRebalancer(ctx, n.cfg.Routing.RebalanceInterval, n.pool,
				n.cfg.Routing.RebalanceThreshold, n.cfg.Routing.RebalanceMaxSweep)
		}
	}
	// Always start the head watcher — it has two responsibilities:
	//   1. Feed the pool's BlockCache in real time from SSE head events so
	//      the canonical-head-aware router (SelectForPathPreferCanonicalHead)
	//      has current data. This applies to all networks regardless of
	//      whether response caching is enabled.
	//   2. Invalidate and pre-warm named-head cache entries. This part is a
	//      no-op on networks where n.cache is nil.
	n.headWatcher = startHeadWatcher(ctx, n.id, n.pool, n.cache, n.warmHeadCache)
}

// ServeHTTP handles a proxied request for this network.
// r.URL.Path must already have the network prefix stripped.
func (n *Network) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	apiKey := reqctx.APIKeyIDFromRequest(r)

	r, clientUpstream := n.rewriteClientPath(r)
	apiPath := normalizeAPIPath(r.URL.Path)
	observe := func(status int) {
		n.observeMethodStatus(r.Method, apiPath, status)
		n.observeAPIKey(apiKey, r.Method, apiPath, status)
	}
	reject := func(status int, msg string) {
		observe(status)
		http.Error(w, msg, status)
	}

	// Intercept /eth/v1/node/health: respond synthetically so load balancers
	// get an accurate answer without forwarding to a sick node. When a client
	// prefix is present (e.g. /lighthouse/eth/v1/node/health), scope the
	// answer to that upstream subset.
	if r.URL.Path == "/eth/v1/node/health" {
		switch n.pool.NodeHealthStatus(upstream.ParseSelector(clientUpstream)) {
		case upstream.HealthUp:
			w.WriteHeader(http.StatusOK)
		case upstream.HealthDegraded:
			w.WriteHeader(http.StatusPartialContent)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		return
	}

	// Intercept /healthz: return a simple JSON health summary for this network,
	// scoped to the client upstream when a client prefix was present.
	if r.URL.Path == "/healthz" {
		n.serveHealthz(w, clientUpstream)
		return
	}

	// Blocked paths (evaluated on path after client-prefix normalization).
	for _, pat := range n.blocked {
		if pat.MatchString(r.URL.Path) {
			reject(http.StatusForbidden, "forbidden")
			return
		}
	}

	var ruleUpstream string
	var ruleHit bool
	if n.routing != nil {
		deny, ru, hit := n.routing.matchRouteRule(r.Method, r.URL.Path)
		if hit && deny {
			reject(http.StatusForbidden, "forbidden")
			return
		}
		ruleUpstream, ruleHit = ru, hit
	}

	// Global rate limiter
	if n.globalLimiter != nil && !n.globalLimiter.Allow() {
		w.Header().Set("X-Ebeacon-Rate-Limited", "global")
		reject(http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	// Session tracking (rate limiting + stickiness).
	var sess *Session
	if n.sessions != nil {
		sess = n.sessions.Get(r)
		if n.rl.PerIP != nil && !sess.Allow() {
			w.Header().Set("X-Ebeacon-Rate-Limited", "per-ip")
			reject(http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
	}

	// Preferred upstream: explicit directive > route rule > client path > sticky session.
	directive := upstreamDirective(r)
	preferID := ""
	cacheScope := ""
	var requiredSelector upstream.Selector
	if directive != "" {
		requiredSelector = upstream.ParseSelector(directive)
		// Bare known client-type names (e.g. "lighthouse") should behave
		// identically to the "client:lighthouse" prefix and to the path-based
		// /lighthouse/eth/v1/... selector. Promote to a client-type selector
		// only when the pool actually has upstreams of that type, matching the
		// pool-aware logic in inferClientSelectorPath.
		if requiredSelector.ID != "" && upstream.IsKnownClientType(requiredSelector.ID) &&
			n.pool.HasMatching(upstream.Selector{ClientType: strings.ToLower(requiredSelector.ID)}) {
			requiredSelector = upstream.Selector{ClientType: strings.ToLower(requiredSelector.ID)}
		}
		cacheScope = requiredSelector.String()
	} else if ruleHit && ruleUpstream != "" {
		preferID = ruleUpstream
		cacheScope = preferID
	} else if clientUpstream != "" {
		requiredSelector = upstream.ParseSelector(clientUpstream)
		cacheScope = requiredSelector.String()
	} else if sess != nil && n.cfg.Routing.StickySession {
		preferID = sess.StickyUpstream()
	}

	// SSE: stream directly through the fault-tolerant relay.
	if isEventStream(r) {
		n.relay.Serve(w, r, preferID, requiredSelector)
		return
	}

	acceptBinary := acceptPrefersSSZ(r.Header.Get("Accept"))

	// Cache lookup (GET only).
	var cacheKey string
	var cachePolicy interface{ TTL() time.Duration }
	if n.cache != nil && r.Method == http.MethodGet {
		if p := n.cache.Policy(r.Method, r.URL.Path); p != nil {
			cacheKey = buildCacheKey(n.id, r, cacheScope)
			cachePolicy = p
			if entry := n.cache.Get(cacheKey); entry != nil {
				n.observeCacheByMethod(r.Method, apiPath, "hit")
				n.cacheServed.Inc()
				observe(entry.Status())
				copyEndToEndHeaders(w.Header(), entry.Headers())
				w.Header().Set("X-Ebeacon-Cache", "HIT")
				n.writeBody(w, r, entry.Status(), entry.Headers(), entry.Body(), entry)
				return
			}
			n.observeCacheByMethod(r.Method, apiPath, "miss")
		} else {
			n.observeCacheByMethod(r.Method, apiPath, "bypass_policy")
		}
	} else if n.cache != nil {
		n.observeCacheByMethod(r.Method, apiPath, "bypass_method")
	}

	// Buffer the body: forward() builds upstream requests only from these
	// bytes (never r.Body), and retry/hedge/dedup need a replayable copy.
	const maxRequestBody = 32 << 20
	var bodyBytes []byte
	if r.Body != nil && r.Body != http.NoBody {
		var err error
		bodyBytes, err = io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
		if err != nil {
			observe(http.StatusBadRequest)
			n.logFailure(r, bodyBytes, apiPath, "", http.StatusBadRequest, nil, nil, err, 0, "request_body_read_error", "", 0)
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		if len(bodyBytes) > maxRequestBody {
			observe(http.StatusRequestEntityTooLarge)
			n.logFailure(r, nil, apiPath, "", http.StatusRequestEntityTooLarge, nil, nil, nil, 0, "request_body_too_large", "", 0)
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		r.Body.Close() //nolint:errcheck
	}

	// Determine effective failsafe for this request (per-method overrides).
	fs := n.effectiveFailsafe(r.Method, r.URL.Path)

	// Request multiplexing: deduplicate identical concurrent GET/HEAD requests.
	var resp *http.Response
	var u *upstream.Upstream
	var execErr error
	// On the multiplexed path the leader has already read and finalized the body.
	var respBody []byte
	var bodyBuffered bool
	// Settled after the response body is read: a response whose body turns
	// out to be unreadable must not credit a half-open recovery probe. The
	// multiplexed path settles inside the leader closure instead, so cbTok
	// stays zero (a no-op) for those callers.
	var cbTok upstream.CBToken

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		dedupKey := dedupKey(n.id, r.Method, r.URL, bodyBytes, cacheScope, acceptBinary)

		type muxResult struct {
			resp *http.Response
			u    *upstream.Upstream
			body []byte
		}

		ch := n.inflight.DoChan(dedupKey, func() (val interface{}, err error) {
			// A panic here must not escape the closure: singleflight
			// re-panics on a separate goroutine when callers are waiting
			// (go panic(e)), which is unrecoverable and crashes the whole
			// process. Convert it to an error so only this request fails.
			defer func() {
				if p := recover(); p != nil {
					val, err = nil, fmt.Errorf("panic in multiplexed request: %v", p)
					slog.Error("recovered panic in multiplexed request",
						"network", n.id, "method", r.Method, "path", r.URL.Path, "panic", p)
				}
			}()
			// Detach from the caller's request context so one client's
			// disconnect doesn't fail every deduplicated follower. The
			// failsafe timeout still applies inside executeFS; maxTimeout
			// backstops the case where no failsafe timeout is configured.
			execCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), n.maxTimeout)
			defer cancel()
			resp, u, tok, err := n.executeFS(execCtx, r, bodyBytes, preferID, requiredSelector, fs, apiPath)
			if err != nil {
				return nil, err
			}
			body, err := readAndFinalizeResponseBody(resp, n.maxResponseBytes)
			resp.Body.Close() //nolint:errcheck
			if err != nil {
				tok.Failure()
				return nil, fmt.Errorf("read upstream response body: %w", err)
			}
			settleTokenForStatus(tok, resp.StatusCode)
			return &muxResult{resp: resp, u: u, body: body}, nil
		})

		var res singleflight.Result
		select {
		case res = <-ch:
		case <-r.Context().Done():
			// This caller's client is gone; the shared execution keeps
			// running for any other waiters. Nothing useful can be written.
			observe(statusClientClosedRequest)
			return
		}
		if res.Shared {
			n.multiplexedTotal.Inc()
		}
		if res.Err != nil {
			execErr = res.Err
		} else if res.Val != nil {
			mr := res.Val.(*muxResult)
			// All multiplexed callers share mr.resp, so concurrent calls to
			// finalizeResponseBody (which writes Content-Length into the header
			// map) and copyEndToEndHeaders (which iterates it) would race.
			// Give each goroutine a shallow-copied response with its own header
			// clone to break the aliasing.
			cloned := *mr.resp
			cloned.Header = mr.resp.Header.Clone()
			cloned.Body = http.NoBody
			resp = &cloned
			u = mr.u
			respBody, bodyBuffered = mr.body, true
		} else {
			execErr = fmt.Errorf("multiplexed request failed")
		}
	} else {
		resp, u, cbTok, execErr = n.executeFS(r.Context(), r, bodyBytes, preferID, requiredSelector, fs, apiPath)
	}

	if execErr != nil {
		var selectedErr *selectedUpstreamUnavailableError
		if errors.As(execErr, &selectedErr) {
			slog.Warn("selected upstream unavailable",
				"network", n.id,
				"method", r.Method,
				"path", r.URL.Path,
				"selector", selectedErr.selector,
				"err", execErr)
			n.reqTotal.WithLabelValues("none", "error").Inc()
			observe(http.StatusServiceUnavailable)
			n.logFailure(r, bodyBytes, apiPath, "", http.StatusServiceUnavailable, nil, nil, execErr, time.Since(start), "selected_upstream_unavailable", selectedErr.selector, 0)
			http.Error(w, "selected upstream unavailable", http.StatusServiceUnavailable)
			return
		}
		slog.Error("all upstreams failed",
			"network", n.id, "method", r.Method, "path", r.URL.Path, "err", execErr)
		n.reqTotal.WithLabelValues("none", "error").Inc()
		observe(http.StatusBadGateway)
		n.logFailure(r, bodyBytes, apiPath, "", http.StatusBadGateway, nil, nil, execErr, time.Since(start), "all_upstreams_failed", "", 0)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	if sess != nil && n.cfg.Routing.StickySession {
		sess.SetSticky(u.ID)
	}

	var err error
	if !bodyBuffered {
		respBody, err = readAndFinalizeResponseBody(resp, n.maxResponseBytes)
	}
	if err != nil {
		slog.Error("failed to read upstream response body",
			"network", n.id,
			"method", r.Method,
			"path", r.URL.Path,
			"upstream", u.ID,
			"status", resp.StatusCode,
			"err", err)
		n.reqTotal.WithLabelValues(u.ID, "error").Inc()
		observe(http.StatusBadGateway)
		n.reqDuration.WithLabelValues(u.ID).Observe(time.Since(start).Seconds())
		n.reqDurationByMethod.WithLabelValues(strings.ToUpper(r.Method)).Observe(time.Since(start).Seconds())
		n.reqDurationByPath.WithLabelValues(u.ID, apiPath).Observe(time.Since(start).Seconds())
		if !isClientCancel(r.Context(), err) {
			n.recordUpstreamFailure(u, cbTok, apiPath)
		} else {
			cbTok.Release()
		}
		n.logFailure(r, bodyBytes, apiPath, u.ID, http.StatusBadGateway, resp.Header, nil, err, time.Since(start), "upstream_response_read_error", "", 0)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}

	settleTokenForStatus(cbTok, resp.StatusCode)

	// Cache successful responses after the body has been read in full.
	if cachePolicy != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 &&
		representationMatches(acceptBinary, resp.Header) {
		ttl := n.effectiveCacheTTL(cachePolicy.TTL(), r.URL.Path)
		n.cache.Set(cacheKey, resp.StatusCode, resp.Header, respBody, ttl)
	}

	duration := time.Since(start)
	n.reqTotal.WithLabelValues(u.ID, httpStatusClass(resp.StatusCode)).Inc()
	observe(resp.StatusCode)
	n.reqDuration.WithLabelValues(u.ID).Observe(duration.Seconds())
	n.reqDurationByMethod.WithLabelValues(strings.ToUpper(r.Method)).Observe(duration.Seconds())
	n.reqDurationByPath.WithLabelValues(u.ID, apiPath).Observe(duration.Seconds())

	// Record metrics for score-based routing
	if resp.StatusCode < 500 && !shouldTreatStatusAsPathError(apiPath, resp.StatusCode) {
		u.RecordSuccessForPath(apiPath, duration)
	} else {
		u.RecordScoreErrorForPath(apiPath)
	}
	n.pool.RefreshUpstreamScoreMetrics(u)
	n.pool.RefreshUpstreamPathScoreMetrics(u, apiPath)
	u.RecordResponseStatus(resp.StatusCode)
	if resp.StatusCode >= 400 {
		n.logFailure(r, bodyBytes, apiPath, u.ID, resp.StatusCode, resp.Header, respBody, nil, duration, "proxied_error_response", "", 0)
	}

	w.Header().Set("X-Ebeacon-Upstream", ObfuscateUpstreamID(u.ID))
	if ct := u.ClientType(); ct != "" && ct != "unknown" {
		w.Header().Set("X-Ebeacon-Client-Type", ct)
	}
	copyEndToEndHeaders(w.Header(), resp.Header)
	n.writeBody(w, r, resp.StatusCode, resp.Header, respBody, nil)
}

// writeBody writes status and body, gzip-encoding the body when the client
// accepts it and the response is not already compressed. SSZ
// (application/octet-stream) is skipped: binary payloads compress poorly and
// CL clients requesting SSZ expect raw bytes without a transport re-encoding
// step. A non-nil entry supplies its memoized compressed body.
func (n *Network) writeBody(w http.ResponseWriter, r *http.Request, status int, hdr http.Header, body []byte, entry *cache.Entry) {
	if !n.gzipEnabled || !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") ||
		hdr.Get("Content-Encoding") != "" || len(body) <= 1024 ||
		strings.EqualFold(hdr.Get("Content-Type"), "application/octet-stream") {
		w.WriteHeader(status)
		w.Write(body) //nolint:errcheck
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Del("Content-Length")
	w.WriteHeader(status)
	if entry != nil {
		w.Write(entry.GzipBody(gzipBytes)) //nolint:errcheck
		return
	}
	writeGzip(w, body)
}

func writeGzip(w io.Writer, body []byte) {
	gz := gzipPool.Get().(*gzip.Writer)
	gz.Reset(w)
	gz.Write(body) //nolint:errcheck
	gz.Close()     //nolint:errcheck
	gzipPool.Put(gz)
}

func gzipBytes(body []byte) []byte {
	var buf bytes.Buffer
	writeGzip(&buf, body)
	return buf.Bytes()
}

// effectiveFailsafe returns the failsafe config for a request, checking per-method overrides.
func (n *Network) effectiveFailsafe(method, path string) config.FailsafeConfig {
	m := strings.ToUpper(method)
	for _, fo := range n.failsafeOverrides {
		if fo.matches(m, path) {
			return fo.failsafe
		}
	}
	return n.failsafe
}

func mergeFailsafe(base, override config.FailsafeConfig) config.FailsafeConfig {
	result := base
	// Clone overridden sections: they alias the network config and
	// ApplyFailsafeDefaults mutates in place, so aliasing would race concurrent
	// requests. base's sections are already defaulted and never written.
	if override.Timeout != nil {
		t := *override.Timeout
		result.Timeout = &t
	}
	if override.Retry != nil {
		r := *override.Retry
		result.Retry = &r
	}
	if override.Hedge != nil {
		h := *override.Hedge
		result.Hedge = &h
	}
	if override.CircuitBreaker != nil {
		cb := *override.CircuitBreaker
		result.CircuitBreaker = &cb
	}
	// A path override replaces whole sections with raw (undefaulted) config;
	// default the merged result so an override that sets only retry.maxAttempts
	// still gets a sane delay/backoff instead of a zero-spacing retry storm.
	config.ApplyFailsafeDefaults(&result)
	return result
}

// settleTokenForStatus applies the shared status-to-outcome rule so it cannot
// drift between the callers that settle a response they have consumed.
// Responses discarded unread settle as Release instead: a body never verified
// must not credit a circuit-breaker recovery probe.
func settleTokenForStatus(tok upstream.CBToken, status int) {
	if status >= 500 {
		tok.Failure()
		return
	}
	tok.Success()
}

// isClientCancel reports whether err is a context cancellation propagated
// from the request context (client disconnect) rather than a failsafe
// timeout — timeouts surface as context.DeadlineExceeded and must still
// count against the upstream's circuit breaker and error rate.
func isClientCancel(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled)
}

func (n *Network) executeSelectedFS(ctx context.Context, r *http.Request, bodyBytes []byte, required upstream.Selector, fs config.FailsafeConfig, apiPath string) (*http.Response, *upstream.Upstream, upstream.CBToken, error) {
	if required.ID != "" {
		u := n.pool.ByID(required.ID)
		if u == nil {
			return nil, nil, upstream.CBToken{}, &selectedUpstreamUnavailableError{selector: required.String()}
		}
		resp, tok, err := n.forward(ctx, u, r, bodyBytes)
		if err != nil {
			if isClientCancel(ctx, err) {
				tok.Release()
			} else if !errors.Is(err, upstream.ErrCircuitUnavailable) {
				n.recordUpstreamFailure(u, tok, apiPath)
			}
			return nil, nil, upstream.CBToken{}, &selectedUpstreamUnavailableError{selector: required.String(), err: err}
		}
		if resp.StatusCode >= 500 {
			u.RecordError()
		}
		return resp, u, tok, nil
	}

	ups := n.pool.SelectMatching(required, apiPath, fs.MaxAttempts())
	if len(ups) == 0 {
		return nil, nil, upstream.CBToken{}, &selectedUpstreamUnavailableError{selector: required.String()}
	}
	return n.executeSelectedCandidatesFS(ctx, r, bodyBytes, required, fs, ups, apiPath)
}

func (n *Network) executeSelectedCandidatesFS(ctx context.Context, r *http.Request, bodyBytes []byte, required upstream.Selector, fs config.FailsafeConfig, ups []*upstream.Upstream, apiPath string) (*http.Response, *upstream.Upstream, upstream.CBToken, error) {
	var lastResp *http.Response
	var lastUpstream *upstream.Upstream
	var lastTok upstream.CBToken
	var lastErr error

	// The buffered response's tracked body owns a DecrActive and its token may
	// own a recovery probe; dropping one without the other leaks activeConns
	// or strands the probe.
	dropBuffered := func() {
		if lastResp != nil {
			lastResp.Body.Close() //nolint:errcheck
			lastTok.Release()
			lastResp = nil
		}
	}

	// Client-type and glob selectors can include multiple upstreams of
	// mixed retention (e.g. a pruned local lighthouse and an archive-capable
	// lighthouse fallback). A pruning-shaped 2xx–4xx response from an early
	// candidate should not win over a real 200 from a later one, so we keep
	// iterating past pruning errors and buffer the last one to return only
	// if every remaining candidate also fails.
	target := classifyHistoricalTarget(r.URL.Path)

	for i, u := range ups {
		if i > 0 && fs.Retry != nil {
			delay := retryDelay(fs.Retry, i-1)
			select {
			case <-ctx.Done():
				dropBuffered()
				return nil, nil, upstream.CBToken{}, &selectedUpstreamUnavailableError{selector: required.String(), err: ctx.Err()}
			case <-time.After(delay):
			}
		}

		resp, tok, err := n.forward(ctx, u, r, bodyBytes)
		if err == nil && resp.StatusCode < 500 {
			peeked, peekErr := peekBodyForPruning(resp, target)
			if peekErr != nil {
				slog.Warn("peek body for pruning classification failed", "network", n.id, "upstream", u.ID, "err", peekErr)
			}
			if i < len(ups)-1 && isPruningError(resp.StatusCode, peeked, target) {
				// Pruning-shaped response with more candidates remaining:
				// buffer it (returned only if no later candidate produces a
				// real success) and continue. Its token rides along unsettled
				// so the outcome reflects whether the body is readable, and it
				// is never a failure — a non-supernode client fielding a
				// steady stream of blob queries it cannot serve would
				// otherwise trip its own breaker.
				u.RecordResponseStatus(resp.StatusCode)
				dropBuffered()
				lastResp = resp
				lastUpstream = u
				lastTok = tok
				lastErr = fmt.Errorf("HTTP %d (pruned)", resp.StatusCode)
				continue
			}
			dropBuffered()
			return resp, u, tok, nil
		}

		if err != nil && isClientCancel(ctx, err) {
			tok.Release()
			dropBuffered()
			return nil, nil, upstream.CBToken{}, err
		}
		if errors.Is(err, upstream.ErrCircuitUnavailable) {
			lastErr = err
			continue
		}
		tok.Failure()
		if err != nil || i < len(ups)-1 {
			u.RecordScoreErrorForPath(apiPath)
			n.pool.RefreshUpstreamPathScoreMetrics(u, apiPath)
		}
		u.RecordError()
		if err != nil {
			lastErr = err
			continue
		}
		dropBuffered()
		lastResp = resp
		lastUpstream = u
		lastTok = upstream.CBToken{}
		lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	if lastResp != nil {
		return lastResp, lastUpstream, lastTok, nil
	}
	return nil, nil, upstream.CBToken{}, &selectedUpstreamUnavailableError{selector: required.String(), err: lastErr}
}

// dedupKey computes a hash key for request deduplication. acceptBinary must
// be part of the key: SSZ and JSON requests for the same path are different
// representations and must not share one upstream response.
func dedupKey(networkID, method string, u *url.URL, body []byte, scope string, acceptBinary bool) string {
	key := networkID + ":" + method + ":" + pathAndQueryForUpstream(u)
	if len(body) > 0 {
		h := sha256.Sum256(body)
		key += ":" + fmt.Sprintf("%x", h[:8])
	}
	if acceptBinary {
		key += ":accept=binary"
	}
	if scope != "" {
		key += ":scope=" + scope
	}
	return key
}

// effectiveCacheTTL returns the TTL to use when caching the response for path.
//
// Finality promotion: a numeric slot/epoch that is at or below the current
// finalized checkpoint gets TTL=0 (cached forever) regardless of the policy.
//
// Slot-boundary alignment: when the network has a genesisTime configured and
// the path references a named slot ID (head, finalized, justified), the TTL is
// capped to the time remaining in the current Ethereum slot. This prevents a
// response cached late in slot N from being served as fresh data in slot N+1.
func (n *Network) effectiveCacheTTL(policyTTL time.Duration, path string) time.Duration {
	if isFinalizedPath(path, n.pool.FinalizedEpoch(), n.pool.SlotsPerEpoch()) {
		return 0 // finalized → cache forever
	}
	if policyTTL > 0 && n.cfg.GenesisTime != 0 && pathHasNamedSlotID(path) {
		slotSeconds := n.cfg.SecondsPerSlot
		if slotSeconds <= 0 {
			slotSeconds = config.DefaultSecondsPerSlot
		}
		elapsed := (time.Now().Unix() - n.cfg.GenesisTime) % slotSeconds
		if elapsed < 0 {
			elapsed += slotSeconds
		}
		remaining := time.Duration(slotSeconds-elapsed) * time.Second
		if remaining < time.Second {
			remaining = time.Second
		}
		if remaining < policyTTL {
			return remaining
		}
	}
	return policyTTL
}

// executeFS dispatches the request under the given failsafe config. The
// returned CBToken belongs to the returned response and must be settled by the
// consumer after the body is read: a response whose body turns out to be
// unreadable must not count as a circuit-breaker recovery success. A zero
// token means settlement already happened internally.
func (n *Network) executeFS(ctx context.Context, r *http.Request, bodyBytes []byte, preferID string, required upstream.Selector, fs config.FailsafeConfig, apiPath string) (*http.Response, *upstream.Upstream, upstream.CBToken, error) {
	var timeoutCancel context.CancelFunc
	if fs.Timeout != nil {
		ctx, timeoutCancel = context.WithTimeout(ctx, fs.Timeout.Duration)
	}
	cancelTimeout := func() {
		if timeoutCancel != nil {
			timeoutCancel()
		}
	}

	if required.Enabled() {
		resp, u, tok, err := n.executeSelectedFS(ctx, r, bodyBytes, required, fs, apiPath)
		if err != nil {
			cancelTimeout()
			return nil, nil, upstream.CBToken{}, err
		}
		return wrapResponseBodyCancel(resp, timeoutCancel), u, tok, nil
	}

	// For requests targeting a named head ID (/head, /finalized, /justified)
	// prefer upstreams that have reported the canonical head block. This closes
	// the race window where a downstream client receives an SSE head event from
	// one upstream and immediately queries for the block data before other
	// upstreams have caught up. Detection uses the raw URL path because the
	// normalized apiPath collapses "head" into "{block_id}".
	preferCanonicalHead := pathHasNamedSlotID(r.URL.Path)

	// Archive-aware routing: classify the request path to detect historical
	// targets (slot/epoch/root identifiers on /beacon/blocks, /blob_sidecars,
	// /states, /validator/duties, etc.). When the target is demonstrably older
	// than the per-endpoint retention window AND at least one upstream is
	// marked archive, route directly to archive upstreams on the first attempt.
	// This avoids the wasted roundtrip to a pruned upstream that would 404.
	// The error-driven fallthrough below catches cases this misses (root-based
	// lookups, or retention windows more aggressive than our conservative
	// thresholds).
	target := classifyHistoricalTarget(r.URL.Path)
	proactiveArchive := n.pool.HasArchive() && target.RequiresArchive(
		n.pool.BlockCache().MaxSlot(),
		n.pool.SlotsPerEpoch(),
	)

	// When proactive archive routing is on, neutralize a sticky-session or
	// route-rule preferID that points to a non-archive upstream. Otherwise
	// ensurePreferredUpstreamFirst would prepend a pruned upstream to the
	// archive-only candidate set, defeating the whole point of proactive
	// routing (and also short-circuiting error-driven fallthrough, because
	// archiveBiased starts true). The session affinity is harmless for the
	// current request — if the preferred upstream is archive-capable, it's
	// honored; otherwise it's dropped for this one request only.
	if proactiveArchive && preferID != "" {
		if pu := n.pool.ByID(preferID); pu == nil || !pu.IsArchive() {
			preferID = ""
		}
	}

	selectForPath := n.pool.SelectForPath
	switch {
	case proactiveArchive:
		selectForPath = n.pool.SelectForPathArchive
		metricArchivePromotion.WithLabelValues(n.id, archivePromotionProactive).Inc()
	case preferCanonicalHead:
		selectForPath = n.pool.SelectForPathPreferCanonicalHead
	}

	// Consensus policy: send to N upstreams, require M agreement
	if n.consensus != nil {
		ups := selectForPath(apiPath, n.consensus.MaxParticipants)
		if len(ups) >= n.consensus.AgreementThreshold {
			resp, u, err := n.consensus.Execute(ctx, ups, func(u *upstream.Upstream) (*http.Request, error) {
				req, err := newUpstreamRequest(ctx, u, r.Method, r, bodyBytes)
				if err != nil {
					return nil, err
				}
				// Strip the client's Accept-Encoding so Go's transport
				// negotiates gzip itself and transparently decompresses:
				// consensus compares bodies byte-wise, and per-node gzip
				// output is not deterministic. It also keeps the winning
				// (cached) body plain.
				req.Header.Del("Accept-Encoding")
				return req, nil
			})
			if err == nil {
				return wrapResponseBodyCancel(resp, timeoutCancel), u, upstream.CBToken{}, nil
			}
			// Recovery probes gated enough participants to make the agreement
			// threshold unreachable; the sequential path below can still serve.
			if !errors.Is(err, upstream.ErrCircuitUnavailable) {
				cancelTimeout()
				return nil, nil, upstream.CBToken{}, err
			}
		}
	}

	maxAttempts := fs.MaxAttempts()

	// Hedge: fire parallel requests after a delay.
	if fs.Hedge != nil {
		count := fs.Hedge.MaxCount + 1
		ups := selectForPath(apiPath, count)
		if len(ups) > 1 {
			resp, u, tok, err := n.executeHedgeFS(ctx, ups, r, bodyBytes, preferID, fs, apiPath, target, proactiveArchive)
			if err != nil {
				cancelTimeout()
				return nil, nil, upstream.CBToken{}, err
			}
			return wrapResponseBodyCancel(resp, timeoutCancel), u, tok, nil
		}
	}

	// Sequential retry across upstreams.
	ups := selectForPath(apiPath, maxAttempts)
	if len(ups) == 0 {
		cancelTimeout()
		return nil, nil, upstream.CBToken{}, fmt.Errorf("no upstreams available")
	}

	ups = ensurePreferredUpstreamFirst(n.pool, ups, preferID, maxAttempts)

	// archiveBiased tracks whether the current ups slice is already archive-only.
	// Starts true if we proactively routed to archive (no further promotion needed),
	// or flips true when a pruning-shaped 404 triggers mid-loop promotion.
	archiveBiased := proactiveArchive
	// triedByID dedupes upstreams across the pre-swap and post-swap portions of
	// the attempt sequence so archive promotion doesn't retry an upstream we
	// already exhausted (archive upstream that also appears in normal ordering).
	triedByID := make(map[string]struct{}, len(ups))

	var lastErr error
	for i := 0; i < len(ups); i++ {
		u := ups[i]
		if _, already := triedByID[u.ID]; already {
			continue
		}
		triedByID[u.ID] = struct{}{}

		if i > 0 && fs.Retry != nil {
			delay := retryDelay(fs.Retry, i-1)
			select {
			case <-ctx.Done():
				cancelTimeout()
				return nil, nil, upstream.CBToken{}, ctx.Err()
			case <-time.After(delay):
			}
		}

		attemptStarted := time.Now()
		resp, tok, err := n.forward(ctx, u, r, bodyBytes)
		if err == nil && resp.StatusCode < 500 {
			// Pruning-shaped response on a historical target: if we have
			// archive upstreams we haven't yet tried, promote and re-enter
			// the loop with archive candidates filling the remaining attempt
			// budget. Peek a bounded prefix of the body so we can recognize
			// post-Fusaka HTTP 400 "insufficient data columns" custody errors
			// as pruning-shaped; the peek is replayed to the client on the
			// no-archive fallthrough.
			peekedBody, peekErr := peekBodyForPruning(resp, target)
			if peekErr != nil {
				slog.Warn("peek body for pruning classification failed", "network", n.id, "upstream", u.ID, "err", peekErr)
			}
			if !archiveBiased && isPruningError(resp.StatusCode, peekedBody, target) {
				if !n.pool.HasArchive() {
					// Pruning-shaped 404 with no archive upstream configured.
					// Surface this via a metric so operators can see the
					// signal that adding an archive upstream would help,
					// then return the 404 unchanged (no behavior change).
					metricPruningErrorNoArchive.WithLabelValues(n.id).Inc()
					return wrapResponseBodyCancel(resp, timeoutCancel), u, tok, nil
				}
				remaining := max(maxAttempts-(i+1), 1)
				archiveUps := n.pool.SelectForPathArchive(apiPath, remaining)
				if len(archiveUps) > 0 {
					resp.Body.Close() //nolint:errcheck
					tok.Release()
					u.RecordResponseStatus(resp.StatusCode)
					metricArchivePromotion.WithLabelValues(n.id, archivePromotionOnError).Inc()
					slog.Debug("promoting to archive upstreams after pruning-shaped 404", "network", n.id, "upstream", u.ID, "status", resp.StatusCode, "api_path", apiPath, "remaining_attempts", len(archiveUps))
					ups = append(ups[:i+1], archiveUps...)
					archiveBiased = true
					lastErr = fmt.Errorf("HTTP %d (pruned)", resp.StatusCode)
					continue
				}
				// Archive upstreams exist but none selectable right now (all
				// unhealthy / circuit-broken / already tried). Fall through to
				// the normal success return with the body still open; the
				// peeked prefix replays via the MultiReader.
			}
			return wrapResponseBodyCancel(resp, timeoutCancel), u, tok, nil
		}

		if err != nil && isClientCancel(ctx, err) {
			tok.Release()
			cancelTimeout()
			return nil, nil, upstream.CBToken{}, err
		}
		if errors.Is(err, upstream.ErrCircuitUnavailable) {
			lastErr = err
			continue
		}
		if err != nil {
			slog.Warn("upstream error", "network", n.id, "upstream", u.ID, "attempt", i+1, "err", err)
			n.logFailure(r, bodyBytes, apiPath, u.ID, 0, nil, nil, err, time.Since(attemptStarted), "upstream_attempt_failed", "", i+1)
			lastErr = err
		} else {
			slog.Warn("upstream bad status", "network", n.id, "upstream", u.ID, "attempt", i+1, "status", resp.StatusCode)
			failedBody, readErr := readAndFinalizeResponseBody(resp, n.maxResponseBytes)
			resp.Body.Close() //nolint:errcheck
			n.logFailure(r, bodyBytes, apiPath, u.ID, resp.StatusCode, resp.Header, failedBody, readErr, time.Since(attemptStarted), "upstream_attempt_failed", "", i+1)
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		n.recordUpstreamFailure(u, tok, apiPath)
	}

	cancelTimeout()
	return nil, nil, upstream.CBToken{}, fmt.Errorf("all %d upstream(s) failed: %w", len(ups), lastErr)
}

// ensurePreferredUpstreamFirst moves preferID to the front, or prepends it from the pool if missing from ups.
func ensurePreferredUpstreamFirst(pool *upstream.Pool, ups []*upstream.Upstream, preferID string, maxLen int) []*upstream.Upstream {
	if preferID == "" || len(ups) == 0 {
		return ups
	}
	for i, u := range ups {
		if u.ID == preferID {
			if pool.BlockCache().IsForked(u.ID) {
				return ups
			}
			if i != 0 {
				ups[0], ups[i] = ups[i], ups[0]
			}
			return trimUpstreamSlice(ups, maxLen)
		}
	}
	if pu := pool.ByID(preferID); pu != nil && pu.IsReady() && !pool.BlockCache().IsForked(pu.ID) {
		out := append([]*upstream.Upstream{pu}, ups...)
		return trimUpstreamSlice(out, maxLen)
	}
	return ups
}

func trimUpstreamSlice(ups []*upstream.Upstream, maxLen int) []*upstream.Upstream {
	if maxLen > 0 && len(ups) > maxLen {
		return ups[:maxLen]
	}
	return ups
}

func (n *Network) executeHedgeFS(ctx context.Context, ups []*upstream.Upstream, r *http.Request, bodyBytes []byte, preferID string, fs config.FailsafeConfig, apiPath string, target HistoricalTarget, archiveBiased bool) (*http.Response, *upstream.Upstream, upstream.CBToken, error) {
	hedge := fs.Hedge
	maxFire := hedge.MaxCount + 1
	if maxFire > len(ups) {
		maxFire = len(ups)
	}
	ups = ensurePreferredUpstreamFirst(n.pool, ups, preferID, maxFire)

	type result struct {
		resp     *http.Response
		u        *upstream.Upstream
		tok      upstream.CBToken
		err      error
		idx      int
		duration time.Duration
	}

	resultCh := make(chan result, maxFire)
	cancels := make([]context.CancelFunc, 0, maxFire)

	fire := func(u *upstream.Upstream) {
		reqCtx, cancel := context.WithCancel(ctx)
		idx := len(cancels)
		cancels = append(cancels, cancel)
		go func(idx int) {
			started := time.Now()
			resp, tok, err := n.forward(reqCtx, u, r, bodyBytes)
			resultCh <- result{resp: resp, u: u, tok: tok, err: err, idx: idx, duration: time.Since(started)}
		}(idx)
	}

	cancelAll := func() {
		for _, c := range cancels {
			c()
		}
	}
	cancelAllExcept := func(keep int) {
		for i, c := range cancels {
			if i == keep {
				continue
			}
			c()
		}
	}
	// drainResults receives the remaining in-flight results and closes their
	// bodies so TrackResponse accounting (DecrActive) completes for losing
	// attempts — context cancellation tears down the connection but never
	// calls the tracked body's Close, which would leak activeConns forever.
	// Release (not Failure) because losers canceled by us are not upstream
	// faults, but it must be unconditional: a loser that errored without a
	// response still holds an admission token, and stranding it would leave
	// a half-open recovery probe in flight forever.
	drainResults := func(remaining int) {
		if remaining <= 0 {
			return
		}
		go func() {
			for range remaining {
				res := <-resultCh
				if res.resp != nil {
					res.resp.Body.Close() //nolint:errcheck
				}
				res.tok.Release()
			}
		}()
	}

	fire(ups[0])
	fired, inflight := 1, 1

	hedgeTimer := time.NewTimer(hedge.Delay)
	defer hedgeTimer.Stop()

	var lastErr error
	// pruningResp buffers the first pruning-shaped response so it doesn't win
	// the hedge race ahead of a potentially-still-inflight 2xx from another
	// upstream. If all hedged attempts complete without a real success, we
	// promote to archive upstreams (sequential pass); only if archive also
	// fails do we fall back to returning the buffered pruning response.
	var pruningResp *result
	// triedByID dedupes upstreams between hedge and the archive-promotion
	// pass so a hedged upstream that also appears in the archive candidate
	// set is not retried against itself.
	triedByID := make(map[string]struct{}, maxFire)

loop:
	for inflight > 0 || fired < maxFire {
		select {
		case res := <-resultCh:
			inflight--
			if res.err == nil && res.resp.StatusCode < 500 {
				// Classify for pruning: peek a bounded prefix of the body so
				// we catch post-Fusaka 400s as well as 404s. The peek is
				// replayed if we end up returning this response to the
				// client.
				peeked, peekErr := peekBodyForPruning(res.resp, target)
				if peekErr != nil {
					slog.Warn("peek hedge response for pruning classification failed", "network", n.id, "upstream", res.u.ID, "err", peekErr)
				}
				if !archiveBiased && n.pool.HasArchive() && isPruningError(res.resp.StatusCode, peeked, target) {
					// Buffer the first pruning-shaped response; discard
					// subsequent ones (they're shaped the same). Don't
					// return yet — another hedged upstream may still yield
					// a real 2xx.
					if pruningResp == nil {
						r := res
						pruningResp = &r
						triedByID[res.u.ID] = struct{}{}
					} else {
						res.resp.Body.Close() //nolint:errcheck
						triedByID[res.u.ID] = struct{}{}
						res.tok.Release()
					}
					res.u.RecordResponseStatus(res.resp.StatusCode)
					if inflight == 0 && fired >= maxFire {
						break loop
					}
					continue
				}
				// Real success (or a non-pruning 4xx) — cancel the rest and
				// return. If we had a buffered pruning response, close it.
				cancelAllExcept(res.idx)
				drainResults(inflight)
				if pruningResp != nil {
					pruningResp.resp.Body.Close() //nolint:errcheck
					pruningResp.tok.Release()
					pruningResp = nil
				}
				res.resp = wrapResponseBodyCancel(res.resp, cancels[res.idx])
				return res.resp, res.u, res.tok, nil
			}
			if res.err != nil && isClientCancel(ctx, res.err) {
				res.tok.Release()
				triedByID[res.u.ID] = struct{}{}
				lastErr = res.err
				if inflight == 0 && fired >= maxFire {
					break loop
				}
				continue
			}
			if errors.Is(res.err, upstream.ErrCircuitUnavailable) {
				triedByID[res.u.ID] = struct{}{}
				lastErr = res.err
				if inflight == 0 && fired >= maxFire {
					break loop
				}
				continue
			}
			if res.err != nil {
				n.logFailure(r, bodyBytes, apiPath, res.u.ID, 0, nil, nil, res.err, res.duration, "hedged_attempt_failed", "", res.idx+1)
				lastErr = res.err
			} else {
				failedBody, readErr := readAndFinalizeResponseBody(res.resp, n.maxResponseBytes)
				lastErr = fmt.Errorf("HTTP %d", res.resp.StatusCode)
				res.resp.Body.Close() //nolint:errcheck
				n.logFailure(r, bodyBytes, apiPath, res.u.ID, res.resp.StatusCode, res.resp.Header, failedBody, readErr, res.duration, "hedged_attempt_failed", "", res.idx+1)
			}
			n.recordUpstreamFailure(res.u, res.tok, apiPath)
			triedByID[res.u.ID] = struct{}{}
			if inflight == 0 && fired >= maxFire {
				break loop
			}

		case <-hedgeTimer.C:
			if fired < maxFire {
				fire(ups[fired])
				fired++
				inflight++
				if fired < maxFire {
					hedgeTimer.Reset(hedge.Delay)
				}
			}

		case <-ctx.Done():
			cancelAll()
			drainResults(inflight)
			if pruningResp != nil {
				pruningResp.resp.Body.Close() //nolint:errcheck
				pruningResp.tok.Release()
			}
			return nil, nil, upstream.CBToken{}, ctx.Err()
		}
	}

	// All hedges complete. If one was a pruning-shaped response, promote to
	// archive upstreams in a sequential pass; if any succeed, return that
	// response and discard the buffered pruning one. If archive promotion
	// also fails (or no archive upstreams are selectable), return the
	// buffered pruning response to the client unchanged.
	if pruningResp != nil {
		// Buffer the pruning body before cancelAll: canceling the request
		// context that produced it closes its transport body asynchronously,
		// which would turn the buffered 404 into a read-error 502.
		pruneBody, pruneReadErr := readAndFinalizeResponseBody(pruningResp.resp, n.maxResponseBytes)
		pruningResp.resp.Body.Close() //nolint:errcheck
		// Settled only now that the body has been read: a pruning-shaped
		// response is not an upstream fault, but a truncated one is, and
		// crediting at buffer time would let it close a half-open circuit.
		if pruneReadErr != nil {
			pruningResp.tok.Failure()
		} else {
			pruningResp.tok.Success()
		}
		cancelAll()
		maxArchiveAttempts := maxFire
		maxArchiveAttempts = max(maxArchiveAttempts, fs.MaxAttempts())
		// The sequential archive pass runs under the parent request ctx so
		// the outer failsafe timeout continues to apply. If hedge consumed
		// most of that budget racing two upstreams, the archive retry may
		// not complete — callers see ctx-deadline. Acceptable tradeoff:
		// keeps the client's overall deadline honored.
		archiveResp, archiveU, archiveTok, archiveErr := n.promoteToArchive(ctx, apiPath, r, bodyBytes, triedByID, maxArchiveAttempts, target)
		if archiveErr == nil && archiveResp != nil {
			metricArchivePromotion.WithLabelValues(n.id, archivePromotionOnError).Inc()
			slog.Debug("promoting hedge pruning result to archive upstream", "network", n.id, "api_path", apiPath, "archive_upstream", archiveU.ID)
			return archiveResp, archiveU, archiveTok, nil
		}
		if archiveErr != nil {
			slog.Debug("archive promotion after hedge pruning failed", "network", n.id, "api_path", apiPath, "err", archiveErr)
		}
		if pruneReadErr != nil {
			return nil, nil, upstream.CBToken{}, fmt.Errorf("read hedge pruning response body: %w", pruneReadErr)
		}
		pruningResp.resp.Body = io.NopCloser(bytes.NewReader(pruneBody))
		return pruningResp.resp, pruningResp.u, upstream.CBToken{}, nil
	}

	cancelAll()
	return nil, nil, upstream.CBToken{}, fmt.Errorf("all hedged requests failed: %w", lastErr)
}

// promoteToArchive runs a sequential pass over the archive-capable upstreams
// not already tried by the caller, returning the first successful (non-5xx,
// non-pruning) response and its unsettled CBToken. Returns a nil response if
// no archive upstreams are available or all fail. Caller is responsible for
// closing any buffered non-archive response if this succeeds.
func (n *Network) promoteToArchive(ctx context.Context, apiPath string, r *http.Request, bodyBytes []byte, triedByID map[string]struct{}, maxAttempts int, target HistoricalTarget) (*http.Response, *upstream.Upstream, upstream.CBToken, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	archiveUps := n.pool.SelectForPathArchive(apiPath, maxAttempts)
	if len(archiveUps) == 0 {
		return nil, nil, upstream.CBToken{}, nil
	}
	var lastErr error
	for _, u := range archiveUps {
		if _, already := triedByID[u.ID]; already {
			continue
		}
		resp, tok, err := n.forward(ctx, u, r, bodyBytes)
		if err != nil {
			lastErr = err
			if isClientCancel(ctx, err) {
				tok.Release()
				return nil, nil, upstream.CBToken{}, err
			}
			if errors.Is(err, upstream.ErrCircuitUnavailable) {
				continue
			}
			tok.Failure()
			continue
		}
		if resp.StatusCode >= 500 {
			resp.Body.Close() //nolint:errcheck
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			tok.Failure()
			continue
		}
		// Archive upstreams still subject to pruning classification —
		// if they also return pruning-shaped, treat as failure and
		// continue. (Rare: archive upstream lacking the blob data.)
		peeked, _ := peekBodyForPruning(resp, target)
		if isPruningError(resp.StatusCode, peeked, target) {
			resp.Body.Close() //nolint:errcheck
			lastErr = fmt.Errorf("archive upstream returned pruning-shaped HTTP %d", resp.StatusCode)
			tok.Release() // discarded unread: not a fault, but no verified success either
			continue
		}
		return resp, u, tok, nil
	}
	return nil, nil, upstream.CBToken{}, lastErr
}

// forward dispatches one request to u and never settles the returned CBToken;
// the caller must settle it exactly once (Success/Failure/Release), including
// on error returns.
func (n *Network) forward(ctx context.Context, u *upstream.Upstream, r *http.Request, body []byte) (*http.Response, upstream.CBToken, error) {
	req, err := newUpstreamRequest(ctx, u, r.Method, r, body)
	if err != nil {
		return nil, upstream.CBToken{}, fmt.Errorf("build request: %w", upstream.SanitizeError(err))
	}

	// Request gzip from upstream if enabled and not SSE
	if n.gzipEnabled && !isEventStream(r) {
		req.Header.Set("Accept-Encoding", "gzip")
	}

	// Append the immediate peer to the XFF chain, per convention. Appending
	// the extracted client IP instead would duplicate the (spoofable) first
	// XFF entry and lose the real peer address. Read the existing chain from
	// the filtered outbound headers, not r.Header, so a client nominating
	// X-Forwarded-For via Connection cannot smuggle it past the hop-by-hop
	// stripping in copyEndToEndHeaders.
	peer := remoteAddrHost(r)
	if existing := req.Header.Get("X-Forwarded-For"); existing != "" {
		req.Header.Set("X-Forwarded-For", existing+", "+peer)
	} else {
		req.Header.Set("X-Forwarded-For", peer)
	}
	req.Header.Set("X-Forwarded-Host", r.Host)

	resp, tok, err := u.Do(req)
	if err != nil {
		return nil, tok, err
	}

	// Decompress gzip upstream response so caching and body processing work correctly.
	// NewReader consumes leading bytes before failing, so a failure cannot fall
	// back to forwarding the (now truncated) body — treat it as an upstream error.
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, gErr := gzip.NewReader(resp.Body)
		if gErr != nil {
			resp.Body.Close() //nolint:errcheck
			return nil, tok, fmt.Errorf("invalid gzip response body: %w", gErr)
		}
		resp.Body = &gzipReadCloser{gzip: gr, orig: resp.Body}
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
	}

	return resp, tok, nil
}

// newUpstreamRequest builds the request to u for client request r, copying
// the client's end-to-end headers and applying u's configured headers.
func newUpstreamRequest(ctx context.Context, u *upstream.Upstream, method string, r *http.Request, body []byte) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.URL+pathAndQueryForUpstream(r.URL), reader)
	if err != nil {
		return nil, err
	}
	copyEndToEndHeaders(req.Header, r.Header)
	for k, v := range u.Headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

// recordUpstreamFailure settles tok as a failure and charges the attempt to
// u's route score and error metrics.
func (n *Network) recordUpstreamFailure(u *upstream.Upstream, tok upstream.CBToken, apiPath string) {
	tok.Failure()
	u.RecordScoreErrorForPath(apiPath)
	n.pool.RefreshUpstreamPathScoreMetrics(u, apiPath)
	u.RecordError()
}

func (n *Network) logFailure(r *http.Request, reqBody []byte, apiPath, upstreamID string, status int, responseHeaders http.Header, responseBody []byte, err error, duration time.Duration, kind, selector string, attempt int) {
	logger := debuglog.Default()
	if !logger.Enabled() {
		return
	}
	logger.LogEvent(debuglog.Event{
		Kind:            kind,
		Network:         n.id,
		APIPath:         apiPath,
		Method:          r.Method,
		Path:            r.URL.Path,
		RawQuery:        r.URL.RawQuery,
		ClientIP:        ClientIP(r),
		Upstream:        upstreamID,
		Selector:        selector,
		Status:          status,
		Attempt:         attempt,
		Duration:        duration,
		Err:             err,
		RequestHeaders:  r.Header,
		RequestBody:     reqBody,
		ResponseHeaders: responseHeaders,
		ResponseBody:    responseBody,
	})
}

type gzipReadCloser struct {
	gzip io.ReadCloser
	orig io.ReadCloser
}

func (g *gzipReadCloser) Read(p []byte) (int, error) { return g.gzip.Read(p) }
func (g *gzipReadCloser) Close() error {
	gzipErr := g.gzip.Close()
	origErr := g.orig.Close()
	if origErr != nil {
		return origErr
	}
	return gzipErr
}

// buildCacheKey produces a stable cache key from the request.
// Transport encoding (gzip/plain) is normalized by the cache layer, but
// representation changes like JSON <-> SSZ are not: the proxy does not attempt
// to transcode between them, so octet-stream requests stay in a distinct cache
// namespace.
func buildCacheKey(networkID string, r *http.Request, scope string) string {
	key := networkID + ":" + r.Method + ":" + pathAndQueryForCache(r.URL)
	if acceptPrefersSSZ(r.Header.Get("Accept")) {
		key += ":accept=binary"
	}
	if scope != "" {
		key += ":upstream=" + scope
	}
	return key
}

// acceptPrefersSSZ reports whether the Accept header prefers
// application/octet-stream (SSZ) over application/json per RFC 9110 q-values.
// The beacon-API spec recommends SSZ clients send
// "application/octet-stream;q=1.0,application/json;q=0.9", which an exact
// string match would misclassify as JSON. Ties and wildcards resolve to JSON,
// matching beacon-node defaults.
func acceptPrefersSSZ(accept string) bool {
	if accept == "" {
		return false
	}
	const (
		specWildcard = iota + 1
		specSubWildcard
		specExact
	)
	var qOctet, qJSON float64
	var specOctet, specJSON int
	for part := range strings.SplitSeq(accept, ",") {
		fields := strings.Split(part, ";")
		mediaType := strings.ToLower(strings.TrimSpace(fields[0]))
		q := 1.0
		for _, p := range fields[1:] {
			if v, ok := strings.CutPrefix(strings.TrimSpace(p), "q="); ok {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					q = f
				}
			}
		}
		apply := func(curQ *float64, curSpec *int, spec int) {
			if spec > *curSpec {
				*curSpec = spec
				*curQ = q
			}
		}
		switch mediaType {
		case "application/octet-stream":
			apply(&qOctet, &specOctet, specExact)
		case "application/json":
			apply(&qJSON, &specJSON, specExact)
		case "application/*":
			apply(&qOctet, &specOctet, specSubWildcard)
			apply(&qJSON, &specJSON, specSubWildcard)
		case "*/*":
			apply(&qOctet, &specOctet, specWildcard)
			apply(&qJSON, &specJSON, specWildcard)
		}
	}
	return qOctet > 0 && qOctet > qJSON
}

// representationMatches reports whether a response's Content-Type is
// consistent with the cache key's representation, so an SSZ body is never
// stored under a JSON-keyed entry (or vice versa) — e.g. when an upstream
// content-negotiates differently than the key predicted.
func representationMatches(wantBinary bool, respHeader http.Header) bool {
	isBinary := strings.HasPrefix(strings.ToLower(respHeader.Get("Content-Type")), "application/octet-stream")
	return wantBinary == isBinary
}

func pathAndQueryForCache(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if u.RawQuery == "" {
		return path
	}
	q := forwardableQuery(u)
	// Default node cache policies don't use arbitrary query args; drop all query
	// params to prevent cache-key explosion via random noise. /node/peers is the
	// exception: its spec-defined state/direction filters change the response,
	// so keep those (and only those).
	if noisyCacheQueryPathRe.MatchString(path) {
		return path
	}
	if peersCacheQueryPathRe.MatchString(path) {
		filtered := url.Values{}
		for _, k := range []string{"state", "direction"} {
			if vs, ok := q[k]; ok {
				filtered[k] = vs
			}
		}
		if enc := filtered.Encode(); enc != "" {
			return path + "?" + enc
		}
		return path
	}
	if enc := q.Encode(); enc != "" {
		return path + "?" + enc
	}
	return path
}

// upstreamDirective returns an upstream selector from the request.
// Clients may use header X-EBEACON-Use-Upstream or query ?use-upstream=.
// The value may be an exact upstream ID, client selector (client:nimbus), or a glob pattern.
func upstreamDirective(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-EBEACON-Use-Upstream")); v != "" {
		return v
	}
	if v := strings.TrimSpace(r.URL.Query().Get("use-upstream")); v != "" {
		return v
	}
	return ""
}

func pathAndQueryForUpstream(u *url.URL) string {
	// EscapedPath keeps percent-encoded segment content (%2F, %3F, %23)
	// intact; the decoded path would be re-parsed by the outbound request
	// and change which resource the upstream sees.
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if u.RawQuery == "" {
		return path
	}
	if enc := forwardableQuery(u).Encode(); enc != "" {
		return path + "?" + enc
	}
	return path
}

// forwardableQuery returns the query without eBeacon-internal parameters.
func forwardableQuery(u *url.URL) url.Values {
	q := u.Query()
	q.Del("secret")
	q.Del("use-upstream")
	q.Del("token")
	return q
}

func isEventStream(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "text/event-stream") ||
		strings.HasSuffix(strings.TrimRight(r.URL.Path, "/"), "/events")
}

// copyEndToEndHeaders copies src into dst without hop-by-hop headers. It is
// used in both directions, so consensus headers such as Eth-Consensus-Version
// pass through unchanged.
func copyEndToEndHeaders(dst, src http.Header) {
	skip := hopByHopHeadersFor(src)
	for k, vv := range src {
		if _, blocked := skip[strings.ToLower(k)]; blocked {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// hopByHopHeadersFor returns the headers to strip: the static set plus any
// nominated by this message's Connection header (RFC 7230 §6.1). The returned
// map is read-only — the common case shares the static set rather than
// allocating a copy per request.
func hopByHopHeadersFor(headers http.Header) map[string]struct{} {
	var nominated map[string]struct{}
	for _, value := range headers.Values("Connection") {
		for token := range strings.SplitSeq(value, ",") {
			name := strings.ToLower(strings.TrimSpace(token))
			if name == "" {
				continue
			}
			if _, static := hopByHopHeaderSet[name]; static {
				continue
			}
			if nominated == nil {
				nominated = maps.Clone(hopByHopHeaderSet)
			}
			nominated[name] = struct{}{}
		}
	}
	if nominated == nil {
		return hopByHopHeaderSet
	}
	return nominated
}

// ObfuscateUpstreamID returns a stable 8-character hex token for an upstream ID
// using FNV-1a. The mapping is one-way, but deterministic — the same ID always
// produces the same token, so a client can share it in a bug report and we can
// identify the upstream without exposing the actual node address.
func ObfuscateUpstreamID(id string) string {
	if v, ok := obfuscatedIDs.Load(id); ok {
		return v.(string)
	}
	h := fnv.New32a()
	h.Write([]byte(id))
	token := fmt.Sprintf("%08x", h.Sum32())
	obfuscatedIDs.Store(id, token)
	return token
}

// obfuscatedIDs memoizes ObfuscateUpstreamID, which runs on every response.
// Keys are configured upstream IDs, so the map stays small.
var obfuscatedIDs sync.Map

func retryDelay(cfg *config.RetryConfig, attempt int) time.Duration {
	delay := float64(cfg.Delay)
	for i := 0; i < attempt; i++ {
		delay *= cfg.Backoff
	}
	if cfg.MaxDelay > 0 && time.Duration(delay) > cfg.MaxDelay {
		delay = float64(cfg.MaxDelay)
	}
	if cfg.Jitter > 0 {
		delay += rand.Float64() * float64(cfg.Jitter)
	}
	return time.Duration(delay)
}

func (n *Network) observeMethodStatus(method, apiPath string, statusCode int) {
	n.reqByMethod.WithLabelValues(strings.ToUpper(method), httpStatusClass(statusCode)).Inc()
	n.reqByPath.WithLabelValues(apiPath, httpStatusClass(statusCode)).Inc()
}

func (n *Network) observeAPIKey(apiKey, method, apiPath string, statusCode int) {
	if apiKey == "" {
		return
	}
	n.reqByAPIKey.WithLabelValues(apiKey, strings.ToUpper(method), httpStatusClass(statusCode)).Inc()
	n.reqByAPIKeyPath.WithLabelValues(apiKey, apiPath, httpStatusClass(statusCode)).Inc()
}

func httpStatusClass(code int) string {
	switch code / 100 {
	case 1:
		return "1xx"
	case 2:
		return "2xx"
	case 3:
		return "3xx"
	case 4:
		return "4xx"
	case 5:
		return "5xx"
	default:
		return "0xx"
	}
}

func shouldTreatStatusAsPathError(apiPath string, statusCode int) bool {
	switch statusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
	default:
		return false
	}

	switch {
	case strings.Contains(apiPath, "/beacon/rewards/"):
		return true
	case strings.Contains(apiPath, "/beacon/blobs/"):
		return true
	case strings.Contains(apiPath, "/beacon/blob_sidecars/"):
		return true
	default:
		return false
	}
}

func (n *Network) observeCacheByMethod(method, apiPath, result string) {
	n.cacheByMethod.WithLabelValues(strings.ToUpper(method), result).Inc()
	n.cacheByPath.WithLabelValues(apiPath, result).Inc()
}

func finalizeResponseBody(headers http.Header, body []byte) []byte {
	headers.Set("Content-Length", strconv.Itoa(len(body)))
	return body
}

func readAndFinalizeResponseBody(resp *http.Response, limit int64) ([]byte, error) {
	body, err := readBodyCapped(resp.Body, limit)
	if err != nil {
		return nil, err
	}
	if resp.Request != nil && resp.Request.Method == http.MethodHead {
		// HEAD bodies are empty by definition; keep the upstream's real
		// Content-Length instead of overwriting it with 0.
		return body, nil
	}
	return finalizeResponseBody(resp.Header, body), nil
}

func readBodyCapped(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return io.ReadAll(r)
	}
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("upstream response body exceeds %d bytes", limit)
	}
	return body, nil
}

// peekBodyForPruning reads up to peerDASBodyPeekLimit bytes of resp.Body so
// isPruningError can inspect it for PeerDAS custody-error substrings (HTTP
// 400 on blob endpoints). Subsequent readers of resp.Body see the original
// byte sequence unchanged — the peeked prefix is replayed via a MultiReader
// and the original Close is preserved. Returns nil for statuses/targets that
// don't require body inspection.
func peekBodyForPruning(resp *http.Response, target HistoricalTarget) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}
	if resp.StatusCode != 400 || target.Kind != HistoricalKindBlobSidecars || !target.IsHistorical() || target.Named != "" {
		return nil, nil
	}
	peeked, err := io.ReadAll(io.LimitReader(resp.Body, peerDASBodyPeekLimit))
	if err != nil {
		return nil, err
	}
	resp.Body = &peekedBody{
		Reader: io.MultiReader(bytes.NewReader(peeked), resp.Body),
		closer: resp.Body,
	}
	return peeked, nil
}

type peekedBody struct {
	io.Reader
	closer io.Closer
}

func (b *peekedBody) Close() error { return b.closer.Close() }

func wrapResponseBodyCancel(resp *http.Response, cancel context.CancelFunc) *http.Response {
	if resp == nil || resp.Body == nil || cancel == nil {
		return resp
	}
	resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
	return resp
}

type cancelOnCloseBody struct {
	io.ReadCloser
	once   sync.Once
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.once.Do(b.cancel)
	}
	return n, err
}

func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.cancel)
	return err
}
