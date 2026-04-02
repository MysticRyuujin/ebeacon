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
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ebeacon/ebeacon/cache"
	"github.com/ebeacon/ebeacon/config"
	"github.com/ebeacon/ebeacon/debuglog"
	"github.com/ebeacon/ebeacon/reqctx"
	"github.com/ebeacon/ebeacon/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

// hop-by-hop headers that must not be forwarded.
// The first group is the standard set defined in RFC 7230 §6.1.
// The second group is ebeacon-specific: these headers carry client auth tokens
// and upstream-selection hints that are only meaningful to ebeacon itself and
// must never reach the backend beacon nodes.
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailers", "Transfer-Encoding", "Upgrade",
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
		gz, _ := gzip.NewWriterLevel(io.Discard, gzip.DefaultCompression)
		return gz
	},
}
var noisyCacheQueryPathRe = regexp.MustCompile(`^/eth/v\d+/node/(?:peers|peer_count|syncing)$`)

// ethConsensusHeaders are Ethereum Beacon API response headers defined in the
// consensus spec that clients depend on to interpret response bodies correctly.
// For example, Eth-Consensus-Version tells the client which fork (Bellatrix,
// Capella, Deneb, …) the response was encoded for, and Eth-Execution-Payload-Blinded
// indicates whether a block contains a full or blinded execution payload.
// These must survive any header-filtering we do before writing the response.
var ethConsensusHeaders = []string{
	"Eth-Consensus-Version",
	"Eth-Execution-Payload-Value",
	"Eth-Execution-Payload-Blinded",
	"Eth-Execution-Requests-Included",
}

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

	// gzip enabled
	gzipEnabled bool

	reqTotal            *prometheus.CounterVec
	reqByMethod         *prometheus.CounterVec
	reqByPath           *prometheus.CounterVec
	reqByAPIKey         *prometheus.CounterVec
	reqByAPIKeyPath     *prometheus.CounterVec
	cacheByMethod       *prometheus.CounterVec
	cacheByPath         *prometheus.CounterVec
	reqDuration         *prometheus.HistogramVec
	reqDurationByMethod *prometheus.HistogramVec
	reqDurationByPath   *prometheus.HistogramVec
	cacheServed         prometheus.Counter
	multiplexedTotal    prometheus.Counter
}

type compiledFailsafeOverride struct {
	re       *regexp.Regexp
	methods  map[string]bool
	failsafe config.FailsafeConfig
}

type requiredUpstreamSelector struct {
	upstreamID string
	clientType string
	glob       string
}

func (s requiredUpstreamSelector) enabled() bool {
	return s.upstreamID != "" || s.clientType != "" || s.glob != ""
}

func (s requiredUpstreamSelector) label() string {
	if s.clientType != "" {
		return "client:" + s.clientType
	}
	if s.glob != "" {
		return "glob:" + s.glob
	}
	return s.upstreamID
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

	n := &Network{
		id:          cfg.ID,
		cfg:         cfg,
		failsafe:    fs,
		rl:          rl,
		pool:        pool,
		relay:       newSSERelay(cfg.ID, pool),
		gzipEnabled: globalCfg.Server.GzipEnabled(),
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
		re, err := regexp.Compile(fo.PathPattern)
		if err != nil {
			slog.Warn("invalid failsafe override pattern", "network", cfg.ID, "pattern", fo.PathPattern)
			continue
		}
		var methods map[string]bool
		if len(fo.Methods) > 0 {
			methods = make(map[string]bool, len(fo.Methods))
			for _, m := range fo.Methods {
				methods[strings.ToUpper(m)] = true
			}
		}
		n.failsafeOverrides = append(n.failsafeOverrides, compiledFailsafeOverride{
			re: re, methods: methods, failsafe: fo.Failsafe,
		})
	}

	// Consensus policy
	n.consensus = NewConsensusPolicy(fs.Consensus)

	// Prometheus metrics (per-network labels).
	n.reqTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "ebeacon_requests_total",
		Help:        "Proxy requests by upstream and HTTP status class",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"upstream", "status_class"})

	n.reqDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "ebeacon_request_duration_seconds",
		Help:        "End-to-end proxy request duration",
		Buckets:     prometheus.DefBuckets,
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"upstream"})

	n.reqDurationByMethod = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "ebeacon_request_duration_by_method_seconds",
		Help:        "End-to-end proxy request duration by HTTP method",
		Buckets:     prometheus.DefBuckets,
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"method"})

	n.reqDurationByPath = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "ebeacon_request_duration_by_path_seconds",
		Help:        "End-to-end proxy request duration by normalized Beacon API path",
		Buckets:     prometheus.DefBuckets,
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"upstream", "api_path"})

	n.reqByMethod = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "ebeacon_requests_by_method_total",
		Help:        "Proxy requests by HTTP method and status class",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"method", "status_class"})

	n.reqByPath = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "ebeacon_requests_by_path_total",
		Help:        "Proxy requests by normalized Beacon API path and status class",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"api_path", "status_class"})

	n.reqByAPIKey = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "ebeacon_requests_by_api_key_total",
		Help:        "Proxy requests by API key, HTTP method, and status class",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"api_key", "method", "status_class"})

	n.reqByAPIKeyPath = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "ebeacon_requests_by_api_key_path_total",
		Help:        "Proxy requests by API key, normalized Beacon API path, and status class",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"api_key", "api_path", "status_class"})

	n.cacheByMethod = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "ebeacon_cache_requests_by_method_total",
		Help:        "Cache outcome by HTTP method (hit, miss, bypass_method, bypass_policy)",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"method", "cache_result"})

	n.cacheByPath = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "ebeacon_cache_requests_by_path_total",
		Help:        "Cache outcome by normalized Beacon API path",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	}, []string{"api_path", "cache_result"})

	n.cacheServed = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "ebeacon_cache_served_total",
		Help:        "Responses served from cache",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	})

	n.multiplexedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "ebeacon_multiplexed_total",
		Help:        "Requests served via deduplication",
		ConstLabels: prometheus.Labels{"network": cfg.ID},
	})

	return n, nil
}

// Pool returns the upstream pool (used by API handlers).
func (n *Network) Pool() *upstream.Pool { return n.pool }

// ID returns the network identifier.
func (n *Network) ID() string { return n.id }

// HealthStatus returns the best health status across all upstreams in this network.
func (n *Network) HealthStatus() upstream.HealthStatus { return n.pool.NodeHealthStatus() }

func (n *Network) serveHealthz(w http.ResponseWriter, clientUpstream string) {
	var total, up, degraded, down int
	var hs upstream.HealthStatus
	if clientUpstream != "" {
		total, up, degraded, down = n.pool.HealthCountsForSelector(clientUpstream)
		hs = n.pool.NodeHealthStatusForSelector(clientUpstream)
	} else {
		total, up, degraded, down = n.pool.HealthCounts()
		hs = n.pool.NodeHealthStatus()
	}

	var status string
	var code int
	switch hs {
	case upstream.HealthUp:
		status, code = "ok", http.StatusOK
	case upstream.HealthDegraded:
		status, code = "degraded", http.StatusOK
	default:
		status, code = "down", http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"status":%q,"upstreams":%d,"healthy":%d,"degraded":%d,"down":%d}`, //nolint:errcheck
		status, total, up, degraded, down)
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
}

// ServeHTTP handles a proxied request for this network.
// r.URL.Path must already have the network prefix stripped.
func (n *Network) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	apiKey := reqctx.APIKeyIDFromRequest(r)

	r, clientUpstream := n.rewriteClientPath(r)
	apiPath := normalizeAPIPath(r.URL.Path)

	// Intercept /eth/v1/node/health: respond synthetically so load balancers
	// get an accurate answer without forwarding to a sick node. When a client
	// prefix is present (e.g. /lighthouse/eth/v1/node/health), scope the
	// answer to that upstream subset.
	if r.URL.Path == "/eth/v1/node/health" {
		var hs upstream.HealthStatus
		if clientUpstream != "" {
			hs = n.pool.NodeHealthStatusForSelector(clientUpstream)
		} else {
			hs = n.pool.NodeHealthStatus()
		}
		switch hs {
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
			n.observeMethodStatus(r.Method, apiPath, http.StatusForbidden)
			n.observeAPIKey(apiKey, r.Method, apiPath, http.StatusForbidden)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}

	var ruleUpstream string
	var ruleHit bool
	if n.routing != nil {
		deny, ru, hit := n.routing.matchRouteRule(r.Method, r.URL.Path)
		if hit && deny {
			n.observeMethodStatus(r.Method, apiPath, http.StatusForbidden)
			n.observeAPIKey(apiKey, r.Method, apiPath, http.StatusForbidden)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		ruleUpstream, ruleHit = ru, hit
	}

	// Global rate limiter
	if n.globalLimiter != nil && !n.globalLimiter.Allow() {
		w.Header().Set("X-Ebeacon-Rate-Limited", "global")
		n.observeMethodStatus(r.Method, apiPath, http.StatusTooManyRequests)
		n.observeAPIKey(apiKey, r.Method, apiPath, http.StatusTooManyRequests)
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Session tracking (rate limiting + stickiness).
	var sess *Session
	if n.sessions != nil {
		sess = n.sessions.Get(r)
		if n.rl.PerIP != nil && !sess.Allow() {
			w.Header().Set("X-Ebeacon-Rate-Limited", "per-ip")
			n.observeMethodStatus(r.Method, apiPath, http.StatusTooManyRequests)
			n.observeAPIKey(apiKey, r.Method, apiPath, http.StatusTooManyRequests)
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}

	// Preferred upstream: explicit directive > route rule > client path > sticky session.
	directive := upstreamDirective(r)
	preferID := ""
	cacheScope := ""
	var requiredSelector requiredUpstreamSelector
	if directive != "" {
		requiredSelector = requiredSelectorFromValue(directive)
		cacheScope = requiredSelector.label()
	} else if ruleHit && ruleUpstream != "" {
		preferID = ruleUpstream
		cacheScope = preferID
	} else if clientUpstream != "" {
		requiredSelector = requiredSelectorFromValue(clientUpstream)
		cacheScope = requiredSelector.label()
	} else if sess != nil && n.cfg.Routing.StickySession {
		preferID = sess.StickyUpstream()
	}

	// SSE: stream directly through the fault-tolerant relay.
	if isEventStream(r) {
		n.relay.Serve(w, r, preferID, requiredSelector)
		return
	}

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
				n.observeMethodStatus(r.Method, apiPath, entry.Status())
				n.observeAPIKey(apiKey, r.Method, apiPath, entry.Status())
				cachedHeaders := entry.Headers().Clone()
				cachedHeaders.Set("X-Ebeacon-Cache", "HIT")
				copyResponseHeaders(w.Header(), cachedHeaders)
				if n.gzipEnabled && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") &&
					cachedHeaders.Get("Content-Encoding") == "" && len(entry.Body()) > 1024 {
					w.Header().Set("Content-Encoding", "gzip")
					w.Header().Del("Content-Length")
					w.WriteHeader(entry.Status())
					gz := gzipPool.Get().(*gzip.Writer)
					gz.Reset(w)
					gz.Write(entry.Body()) //nolint:errcheck
					gz.Close()             //nolint:errcheck
					gzipPool.Put(gz)
				} else {
					w.WriteHeader(entry.Status())
					w.Write(entry.Body()) //nolint:errcheck
				}
				return
			}
			n.observeCacheByMethod(r.Method, apiPath, "miss")
		} else {
			n.observeCacheByMethod(r.Method, apiPath, "bypass_policy")
		}
	} else if n.cache != nil {
		n.observeCacheByMethod(r.Method, apiPath, "bypass_method")
	}

	// Buffer body for retry / hedge.
	var bodyBytes []byte
	if r.Body != nil && r.Body != http.NoBody && (n.needsBodyBuffer() || debuglog.Default().Enabled()) {
		var err error
		bodyBytes, err = io.ReadAll(io.LimitReader(r.Body, 32<<20))
		if err != nil {
			n.observeMethodStatus(r.Method, apiPath, http.StatusBadRequest)
			n.observeAPIKey(apiKey, r.Method, apiPath, http.StatusBadRequest)
			n.logFailure(r, bodyBytes, apiPath, "", http.StatusBadRequest, nil, nil, err, 0, "request_body_read_error", "", 0)
			http.Error(w, "failed to read request body", http.StatusBadRequest)
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

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		dedupKey := dedupKey(n.id, r.Method, r.URL, bodyBytes, cacheScope)

		type muxResult struct {
			resp *http.Response
			u    *upstream.Upstream
			body []byte
		}

		v, muxErr, shared := n.inflight.Do(dedupKey, func() (interface{}, error) {
			resp, u, err := n.executeWithFailsafe(r.Context(), r, bodyBytes, preferID, requiredSelector, fs, apiPath)
			if err != nil {
				return nil, err
			}
			body, err := readAndFinalizeResponseBody(resp)
			resp.Body.Close() //nolint:errcheck
			if err != nil {
				return nil, fmt.Errorf("read upstream response body: %w", err)
			}
			return &muxResult{resp: resp, u: u, body: body}, nil
		})
		if shared {
			n.multiplexedTotal.Inc()
		}
		if muxErr != nil {
			execErr = muxErr
		} else if v != nil {
			mr := v.(*muxResult)
			// All multiplexed callers share mr.resp, so concurrent calls to
			// finalizeResponseBody (which writes Content-Length into the header
			// map) and copyResponseHeaders (which iterates it) would race.
			// Give each goroutine a shallow-copied response with its own header
			// clone to break the aliasing.
			cloned := *mr.resp
			cloned.Header = mr.resp.Header.Clone()
			cloned.Body = io.NopCloser(bytes.NewReader(mr.body))
			resp = &cloned
			u = mr.u
		} else {
			execErr = fmt.Errorf("multiplexed request failed")
		}
	} else {
		resp, u, execErr = n.executeWithFailsafe(r.Context(), r, bodyBytes, preferID, requiredSelector, fs, apiPath)
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
			n.observeMethodStatus(r.Method, apiPath, http.StatusServiceUnavailable)
			n.observeAPIKey(apiKey, r.Method, apiPath, http.StatusServiceUnavailable)
			n.logFailure(r, bodyBytes, apiPath, "", http.StatusServiceUnavailable, nil, nil, execErr, time.Since(start), "selected_upstream_unavailable", selectedErr.selector, 0)
			http.Error(w, "selected upstream unavailable", http.StatusServiceUnavailable)
			return
		}
		slog.Error("all upstreams failed",
			"network", n.id, "method", r.Method, "path", r.URL.Path, "err", execErr)
		n.reqTotal.WithLabelValues("none", "error").Inc()
		n.observeMethodStatus(r.Method, apiPath, http.StatusBadGateway)
		n.observeAPIKey(apiKey, r.Method, apiPath, http.StatusBadGateway)
		n.logFailure(r, bodyBytes, apiPath, "", http.StatusBadGateway, nil, nil, execErr, time.Since(start), "all_upstreams_failed", "", 0)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	if sess != nil && n.cfg.Routing.StickySession {
		sess.SetSticky(u.ID)
	}

	respBody, err := readAndFinalizeResponseBody(resp)
	if err != nil {
		slog.Error("failed to read upstream response body",
			"network", n.id,
			"method", r.Method,
			"path", r.URL.Path,
			"upstream", u.ID,
			"status", resp.StatusCode,
			"err", err)
		n.reqTotal.WithLabelValues(u.ID, "error").Inc()
		n.observeMethodStatus(r.Method, apiPath, http.StatusBadGateway)
		n.observeAPIKey(apiKey, r.Method, apiPath, http.StatusBadGateway)
		n.reqDuration.WithLabelValues(u.ID).Observe(time.Since(start).Seconds())
		n.reqDurationByMethod.WithLabelValues(strings.ToUpper(r.Method)).Observe(time.Since(start).Seconds())
		n.reqDurationByPath.WithLabelValues(u.ID, apiPath).Observe(time.Since(start).Seconds())
		u.RecordScoreErrorForPath(apiPath)
		n.pool.RefreshUpstreamPathScoreMetrics(u, apiPath)
		u.RecordError()
		u.CBFailure()
		n.logFailure(r, bodyBytes, apiPath, u.ID, http.StatusBadGateway, resp.Header, nil, err, time.Since(start), "upstream_response_read_error", "", 0)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}

	// Cache successful responses after the body has been read in full.
	if cachePolicy != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		ttl := n.effectiveCacheTTL(cachePolicy.TTL(), r.URL.Path)
		n.cache.Set(cacheKey, resp.StatusCode, resp.Header, respBody, ttl)
	}

	duration := time.Since(start)
	n.reqTotal.WithLabelValues(u.ID, httpStatusClass(resp.StatusCode)).Inc()
	n.observeMethodStatus(r.Method, apiPath, resp.StatusCode)
	n.observeAPIKey(apiKey, r.Method, apiPath, resp.StatusCode)
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
	copyResponseHeaders(w.Header(), resp.Header)

	// gzip compression for client if accepted and response is not already compressed
	if n.gzipEnabled && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") &&
		resp.Header.Get("Content-Encoding") == "" && len(respBody) > 1024 {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Del("Content-Length")
		w.WriteHeader(resp.StatusCode)
		gz := gzipPool.Get().(*gzip.Writer)
		gz.Reset(w)
		gz.Write(respBody) //nolint:errcheck
		gz.Close()         //nolint:errcheck
		gzipPool.Put(gz)
	} else {
		w.WriteHeader(resp.StatusCode)
		w.Write(respBody) //nolint:errcheck
	}
}

func requiredSelectorFromValue(id string) requiredUpstreamSelector {
	if strings.HasPrefix(id, "client:") {
		return requiredUpstreamSelector{clientType: strings.TrimPrefix(id, "client:")}
	}
	if strings.ContainsAny(id, "*?[") {
		return requiredUpstreamSelector{glob: id}
	}
	return requiredUpstreamSelector{upstreamID: id}
}

// effectiveFailsafe returns the failsafe config for a request, checking per-method overrides.
func (n *Network) effectiveFailsafe(method, path string) config.FailsafeConfig {
	m := strings.ToUpper(method)
	for _, fo := range n.failsafeOverrides {
		if !fo.re.MatchString(path) {
			continue
		}
		if len(fo.methods) > 0 && !fo.methods[m] {
			continue
		}
		return mergeFailsafe(n.failsafe, fo.failsafe)
	}
	return n.failsafe
}

func mergeFailsafe(base, override config.FailsafeConfig) config.FailsafeConfig {
	result := base
	if override.Timeout != nil {
		result.Timeout = override.Timeout
	}
	if override.Retry != nil {
		result.Retry = override.Retry
	}
	if override.Hedge != nil {
		result.Hedge = override.Hedge
	}
	if override.CircuitBreaker != nil {
		result.CircuitBreaker = override.CircuitBreaker
	}
	return result
}

// executeWithFailsafe runs execute with the given failsafe config.
func (n *Network) executeWithFailsafe(ctx context.Context, r *http.Request, bodyBytes []byte, preferID string, required requiredUpstreamSelector, fs config.FailsafeConfig, apiPath string) (*http.Response, *upstream.Upstream, error) {
	return n.executeFS(ctx, r, bodyBytes, preferID, required, fs, apiPath)
}

func (n *Network) executeSelectedFS(ctx context.Context, r *http.Request, bodyBytes []byte, required requiredUpstreamSelector, fs config.FailsafeConfig, apiPath string) (*http.Response, *upstream.Upstream, error) {
	if required.upstreamID != "" {
		u := n.pool.ByID(required.upstreamID)
		if u == nil {
			return nil, nil, &selectedUpstreamUnavailableError{selector: required.label()}
		}
		var body io.Reader
		if bodyBytes != nil {
			body = bytes.NewReader(bodyBytes)
		}
		resp, err := n.forward(ctx, u, r, body)
		if err != nil {
			u.CBFailure()
			u.RecordScoreErrorForPath(apiPath)
			n.pool.RefreshUpstreamPathScoreMetrics(u, apiPath)
			u.RecordError()
			return nil, nil, &selectedUpstreamUnavailableError{selector: required.label(), err: err}
		}
		if resp.StatusCode < 500 {
			u.CBSuccess()
		} else {
			u.CBFailure()
			u.RecordError()
		}
		return resp, u, nil
	}

	if required.glob != "" {
		maxAttempts := 1
		if fs.Retry != nil {
			maxAttempts = fs.Retry.MaxAttempts
		}
		ups := n.pool.SelectByGlobForPath(required.glob, apiPath, maxAttempts)
		if len(ups) == 0 {
			return nil, nil, &selectedUpstreamUnavailableError{selector: required.label()}
		}
		return n.executeSelectedCandidatesFS(ctx, r, bodyBytes, required, fs, ups, apiPath)
	}

	maxAttempts := 1
	if fs.Retry != nil {
		maxAttempts = fs.Retry.MaxAttempts
	}
	ups := n.pool.SelectByClientTypeForPath(required.clientType, apiPath, maxAttempts)
	if len(ups) == 0 {
		return nil, nil, &selectedUpstreamUnavailableError{selector: required.label()}
	}
	return n.executeSelectedCandidatesFS(ctx, r, bodyBytes, required, fs, ups, apiPath)
}

func (n *Network) executeSelectedCandidatesFS(ctx context.Context, r *http.Request, bodyBytes []byte, required requiredUpstreamSelector, fs config.FailsafeConfig, ups []*upstream.Upstream, apiPath string) (*http.Response, *upstream.Upstream, error) {
	var lastResp *http.Response
	var lastUpstream *upstream.Upstream
	var lastErr error

	for i, u := range ups {
		if i > 0 && fs.Retry != nil {
			delay := retryDelay(fs.Retry, i-1)
			select {
			case <-ctx.Done():
				return nil, nil, &selectedUpstreamUnavailableError{selector: required.label(), err: ctx.Err()}
			case <-time.After(delay):
			}
		}

		var body io.Reader
		if bodyBytes != nil {
			body = bytes.NewReader(bodyBytes)
		}

		resp, err := n.forward(ctx, u, r, body)
		if err == nil && resp.StatusCode < 500 {
			u.CBSuccess()
			if lastResp != nil {
				lastResp.Body.Close() //nolint:errcheck
			}
			return resp, u, nil
		}

		u.CBFailure()
		if err != nil || i < len(ups)-1 {
			u.RecordScoreErrorForPath(apiPath)
			n.pool.RefreshUpstreamPathScoreMetrics(u, apiPath)
		}
		u.RecordError()
		if err != nil {
			lastErr = err
			continue
		}
		if lastResp != nil {
			lastResp.Body.Close() //nolint:errcheck
		}
		lastResp = resp
		lastUpstream = u
		lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	if lastResp != nil {
		return lastResp, lastUpstream, nil
	}
	return nil, nil, &selectedUpstreamUnavailableError{selector: required.label(), err: lastErr}
}

// dedupKey computes a hash key for request deduplication.
func dedupKey(networkID, method string, u *url.URL, body []byte, scope string) string {
	key := networkID + ":" + method + ":" + pathAndQueryForUpstream(u)
	if len(body) > 0 {
		h := sha256.Sum256(body)
		key += ":" + fmt.Sprintf("%x", h[:8])
	}
	if scope != "" {
		key += ":scope=" + scope
	}
	return key
}

// effectiveCacheTTL returns TTL=0 (forever) when the request references a finalized slot.
func (n *Network) effectiveCacheTTL(policyTTL time.Duration, path string) time.Duration {
	finalizedSlot := n.pool.FinalizedSlot()
	if finalizedSlot == 0 {
		return policyTTL
	}
	if slot, ok := pathNumericSlot(path); ok && slot <= finalizedSlot {
		return 0 // finalized → cache forever
	}
	if epoch, ok := pathNumericEpoch(path); ok && epoch <= finalizedSlot/32 {
		return 0 // finalized epoch → cache forever
	}
	return policyTTL
}

func (n *Network) executeFS(ctx context.Context, r *http.Request, bodyBytes []byte, preferID string, required requiredUpstreamSelector, fs config.FailsafeConfig, apiPath string) (*http.Response, *upstream.Upstream, error) {
	var timeoutCancel context.CancelFunc
	if fs.Timeout != nil {
		ctx, timeoutCancel = context.WithTimeout(ctx, fs.Timeout.Duration)
	}
	cancelTimeout := func() {
		if timeoutCancel != nil {
			timeoutCancel()
		}
	}

	if required.enabled() {
		resp, u, err := n.executeSelectedFS(ctx, r, bodyBytes, required, fs, apiPath)
		if err != nil {
			cancelTimeout()
			return nil, nil, err
		}
		return wrapResponseBodyCancel(resp, timeoutCancel), u, nil
	}

	// Consensus policy: send to N upstreams, require M agreement
	if n.consensus != nil {
		ups := n.pool.SelectForPath(apiPath, n.consensus.MaxParticipants)
		if len(ups) >= n.consensus.AgreementThreshold {
			resp, u, err := n.consensus.Execute(ctx, ups, func(u *upstream.Upstream) (*http.Request, error) {
				dest := u.URL + pathAndQueryForUpstream(r.URL)
				var body io.Reader
				if bodyBytes != nil {
					body = bytes.NewReader(bodyBytes)
				}
				req, err := http.NewRequestWithContext(ctx, r.Method, dest, body)
				if err != nil {
					return nil, err
				}
				copyRequestHeaders(req.Header, r.Header)
				for k, v := range u.Headers {
					req.Header.Set(k, v)
				}
				return req, nil
			})
			if err != nil {
				cancelTimeout()
				return nil, nil, err
			}
			return wrapResponseBodyCancel(resp, timeoutCancel), u, nil
		}
	}

	maxAttempts := 1
	if fs.Retry != nil {
		maxAttempts = fs.Retry.MaxAttempts
	}

	// Hedge: fire parallel requests after a delay.
	if fs.Hedge != nil {
		count := fs.Hedge.MaxCount + 1
		ups := n.pool.SelectForPath(apiPath, count)
		if len(ups) > 1 {
			resp, u, err := n.executeHedgeFS(ctx, ups, r, bodyBytes, preferID, fs, apiPath)
			if err != nil {
				cancelTimeout()
				return nil, nil, err
			}
			return wrapResponseBodyCancel(resp, timeoutCancel), u, nil
		}
	}

	// Sequential retry across upstreams.
	ups := n.pool.SelectForPath(apiPath, maxAttempts)
	if len(ups) == 0 {
		cancelTimeout()
		return nil, nil, fmt.Errorf("no upstreams available")
	}

	ups = ensurePreferredUpstreamFirst(n.pool, ups, preferID, maxAttempts)

	var lastErr error
	for i, u := range ups {
		if i > 0 && fs.Retry != nil {
			delay := retryDelay(fs.Retry, i-1)
			select {
			case <-ctx.Done():
				cancelTimeout()
				return nil, nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		var body io.Reader
		if bodyBytes != nil {
			body = bytes.NewReader(bodyBytes)
		}

		attemptStarted := time.Now()
		resp, err := n.forward(ctx, u, r, body)
		if err == nil && resp.StatusCode < 500 {
			u.CBSuccess()
			return wrapResponseBodyCancel(resp, timeoutCancel), u, nil
		}

		if err != nil {
			slog.Warn("upstream error", "network", n.id, "upstream", u.ID, "attempt", i+1, "err", err)
			n.logFailure(r, bodyBytes, apiPath, u.ID, 0, nil, nil, err, time.Since(attemptStarted), "upstream_attempt_failed", "", i+1)
			lastErr = err
		} else {
			slog.Warn("upstream bad status", "network", n.id, "upstream", u.ID, "attempt", i+1, "status", resp.StatusCode)
			failedBody, readErr := readAndFinalizeResponseBody(resp)
			resp.Body.Close() //nolint:errcheck
			if readErr != nil {
				n.logFailure(r, bodyBytes, apiPath, u.ID, resp.StatusCode, resp.Header, nil, readErr, time.Since(attemptStarted), "upstream_attempt_failed", "", i+1)
			} else {
				n.logFailure(r, bodyBytes, apiPath, u.ID, resp.StatusCode, resp.Header, failedBody, nil, time.Since(attemptStarted), "upstream_attempt_failed", "", i+1)
			}
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		u.CBFailure()
		u.RecordScoreErrorForPath(apiPath)
		n.pool.RefreshUpstreamPathScoreMetrics(u, apiPath)
		u.RecordError()
	}

	cancelTimeout()
	return nil, nil, fmt.Errorf("all %d upstream(s) failed: %w", len(ups), lastErr)
}

// ensurePreferredUpstreamFirst moves preferID to the front, or prepends it from the pool if missing from ups.
func ensurePreferredUpstreamFirst(pool *upstream.Pool, ups []*upstream.Upstream, preferID string, maxLen int) []*upstream.Upstream {
	if preferID == "" || len(ups) == 0 {
		return ups
	}
	for i, u := range ups {
		if u.ID == preferID {
			if i != 0 {
				ups[0], ups[i] = ups[i], ups[0]
			}
			return trimUpstreamSlice(ups, maxLen)
		}
	}
	if pu := pool.ByID(preferID); pu != nil && pu.IsReady() {
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

func (n *Network) executeHedgeFS(ctx context.Context, ups []*upstream.Upstream, r *http.Request, bodyBytes []byte, preferID string, fs config.FailsafeConfig, apiPath string) (*http.Response, *upstream.Upstream, error) {
	hedge := fs.Hedge
	maxFire := hedge.MaxCount + 1
	if maxFire > len(ups) {
		maxFire = len(ups)
	}
	ups = ensurePreferredUpstreamFirst(n.pool, ups, preferID, maxFire)

	type result struct {
		resp     *http.Response
		u        *upstream.Upstream
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
			var body io.Reader
			if bodyBytes != nil {
				body = bytes.NewReader(bodyBytes)
			}
			started := time.Now()
			resp, err := n.forward(reqCtx, u, r, body)
			resultCh <- result{resp: resp, u: u, err: err, idx: idx, duration: time.Since(started)}
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

	fire(ups[0])
	fired, inflight := 1, 1

	hedgeTimer := time.NewTimer(hedge.Delay)
	defer hedgeTimer.Stop()

	var lastErr error

	for inflight > 0 || fired < maxFire {
		select {
		case res := <-resultCh:
			inflight--
			if res.err == nil && res.resp.StatusCode < 500 {
				cancelAllExcept(res.idx)
				res.resp = wrapResponseBodyCancel(res.resp, cancels[res.idx])
				res.u.CBSuccess()
				return res.resp, res.u, nil
			}
			if res.err != nil {
				n.logFailure(r, bodyBytes, apiPath, res.u.ID, 0, nil, nil, res.err, res.duration, "hedged_attempt_failed", "", res.idx+1)
				lastErr = res.err
			} else {
				failedBody, readErr := readAndFinalizeResponseBody(res.resp)
				lastErr = fmt.Errorf("HTTP %d", res.resp.StatusCode)
				res.resp.Body.Close() //nolint:errcheck
				if readErr != nil {
					n.logFailure(r, bodyBytes, apiPath, res.u.ID, res.resp.StatusCode, res.resp.Header, nil, readErr, res.duration, "hedged_attempt_failed", "", res.idx+1)
				} else {
					n.logFailure(r, bodyBytes, apiPath, res.u.ID, res.resp.StatusCode, res.resp.Header, failedBody, nil, res.duration, "hedged_attempt_failed", "", res.idx+1)
				}
			}
			res.u.CBFailure()
			res.u.RecordScoreErrorForPath(apiPath)
			n.pool.RefreshUpstreamPathScoreMetrics(res.u, apiPath)
			res.u.RecordError()
			if inflight == 0 && fired >= maxFire {
				cancelAll()
				return nil, nil, fmt.Errorf("all hedged requests failed: %w", lastErr)
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
			return nil, nil, ctx.Err()
		}
	}

	return nil, nil, fmt.Errorf("all hedged requests failed: %w", lastErr)
}

func (n *Network) forward(ctx context.Context, u *upstream.Upstream, r *http.Request, body io.Reader) (*http.Response, error) {
	dest := u.URL + pathAndQueryForUpstream(r.URL)
	req, err := http.NewRequestWithContext(ctx, r.Method, dest, body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	copyRequestHeaders(req.Header, r.Header)
	for k, v := range u.Headers {
		req.Header.Set(k, v)
	}

	// Request gzip from upstream if enabled and not SSE
	if n.gzipEnabled && !isEventStream(r) {
		req.Header.Set("Accept-Encoding", "gzip")
	}

	clientAddr := extractClientIP(r)
	if existing := r.Header.Get("X-Forwarded-For"); existing != "" {
		req.Header.Set("X-Forwarded-For", existing+", "+clientAddr)
	} else {
		req.Header.Set("X-Forwarded-For", clientAddr)
	}
	req.Header.Set("X-Forwarded-Host", r.Host)

	u.IncrActive()
	resp, err := u.Client.Do(req)
	if err != nil {
		u.DecrActive()
		return nil, err
	}

	// Decompress gzip upstream response so caching and body processing work correctly
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, gErr := gzip.NewReader(resp.Body)
		if gErr == nil {
			resp.Body = &gzipReadCloser{gzip: gr, orig: resp.Body}
			resp.Header.Del("Content-Encoding")
			resp.Header.Del("Content-Length")
		}
	}

	return u.TrackResponse(resp), nil
}

func (n *Network) logFailure(r *http.Request, reqBody []byte, apiPath, upstreamID string, status int, responseHeaders http.Header, responseBody []byte, err error, duration time.Duration, kind, selector string, attempt int) {
	debuglog.Default().LogEvent(debuglog.Event{
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

func (n *Network) needsBodyBuffer() bool {
	if n.failsafe.Hedge != nil {
		return true
	}
	if n.failsafe.Retry != nil && n.failsafe.Retry.MaxAttempts > 1 {
		return true
	}
	return false
}

// buildCacheKey produces a stable cache key from the request.
// Transport encoding (gzip/plain) is normalized by the cache layer, but
// representation changes like JSON <-> SSZ are not: the proxy does not attempt
// to transcode between them, so octet-stream requests stay in a distinct cache
// namespace.
func buildCacheKey(networkID string, r *http.Request, scope string) string {
	key := networkID + ":" + r.Method + ":" + pathAndQueryForCache(r.URL)
	if strings.EqualFold(r.Header.Get("Accept"), "application/octet-stream") {
		key += ":accept=binary"
	}
	if scope != "" {
		key += ":upstream=" + scope
	}
	return key
}

func pathAndQueryForCache(u *url.URL) string {
	path := u.Path
	if path == "" {
		path = "/"
	}
	q := u.Query()
	q.Del("secret")
	q.Del("use-upstream")
	q.Del("token")
	// Default node cache policies don't use arbitrary query args; drop all query
	// params to prevent cache-key explosion via random noise.
	if noisyCacheQueryPathRe.MatchString(path) {
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
	path := u.Path
	if path == "" {
		path = "/"
	}
	q := u.Query()
	// Strip eBeacon-internal query params before forwarding to the upstream.
	q.Del("secret")
	q.Del("use-upstream")
	q.Del("token")
	if enc := q.Encode(); enc != "" {
		return path + "?" + enc
	}
	return path
}

func isEventStream(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "text/event-stream") ||
		strings.HasSuffix(strings.TrimRight(r.URL.Path, "/"), "/events")
}

func copyRequestHeaders(dst, src http.Header) {
	for k, vv := range src {
		if isHopByHopHeader(k) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func copyResponseHeaders(dst, src http.Header) {
	for k, vv := range src {
		if isHopByHopHeader(k) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
	// Explicitly preserve Ethereum consensus headers even if they were skipped
	for _, h := range ethConsensusHeaders {
		if v := src.Get(h); v != "" {
			dst.Set(h, v)
		}
	}
}

func isHopByHopHeader(h string) bool {
	_, ok := hopByHopHeaderSet[strings.ToLower(h)]
	return ok
}

func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// ObfuscateUpstreamID returns a stable 8-character hex token for an upstream ID
// using FNV-1a. The mapping is one-way, but deterministic — the same ID always
// produces the same token, so a client can share it in a bug report and we can
// identify the upstream without exposing the actual node address.
func ObfuscateUpstreamID(id string) string {
	h := fnv.New32a()
	h.Write([]byte(id))
	return fmt.Sprintf("%08x", h.Sum32())
}

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

func readAndFinalizeResponseBody(resp *http.Response) ([]byte, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return finalizeResponseBody(resp.Header, body), nil
}

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
