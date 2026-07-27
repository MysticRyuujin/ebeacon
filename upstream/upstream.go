package upstream

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// CBState represents the circuit breaker state.
type CBState int

const (
	CBClosed   CBState = iota // normal operation
	CBOpen                    // rejecting requests
	CBHalfOpen                // probing recovery
)

// HealthStatus represents the observed health of an upstream.
type HealthStatus int

const (
	HealthDown     HealthStatus = iota // unreachable
	HealthDegraded                     // reachable but syncing
	HealthUp                           // fully operational
)

var (
	metricHealth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_health",
		Help: "Upstream health (0=down, 1=degraded, 2=up)",
	}, []string{"network", "upstream"})

	metricCBState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_circuit_breaker_state",
		Help: "Circuit breaker state (0=closed, 1=open, 2=half-open)",
	}, []string{"network", "upstream"})

	metricActiveConns = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_active_connections",
		Help: "Number of in-flight requests to upstream",
	}, []string{"network", "upstream"})

	metricTotalRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_upstream_requests_total",
		Help: "Total requests forwarded to upstream",
	}, []string{"network", "upstream"})

	metricTotalErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_upstream_errors_total",
		Help: "Total upstream request errors",
	}, []string{"network", "upstream"})

	metricHeadSlot = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_head_slot",
		Help: "Latest observed head slot for each upstream",
	}, []string{"network", "upstream"})

	metricSyncDistance = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_sync_distance",
		Help: "Latest observed sync distance for each upstream",
	}, []string{"network", "upstream"})

	metricRoutingScore = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score",
		Help: "Composite upstream routing score (higher is better)",
	}, []string{"network", "upstream"})

	metricRoutingScoreErrorRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score_error_rate",
		Help: "Rolling upstream error rate used in score-based routing",
	}, []string{"network", "upstream"})

	metricRoutingScoreP90Latency = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score_p90_latency_seconds",
		Help: "Rolling upstream p90 latency in seconds used in score-based routing",
	}, []string{"network", "upstream"})

	metricRoutingScoreHeadLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score_head_lag",
		Help: "Head lag in slots used in score-based routing",
	}, []string{"network", "upstream"})

	metricRoutingScoreSamples = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score_samples",
		Help: "Rolling sample count used for upstream score computation",
	}, []string{"network", "upstream"})

	metricRoutingScoreByPath = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score_by_path",
		Help: "Composite upstream routing score by normalized Beacon API path",
	}, []string{"network", "upstream", "api_path"})

	metricRoutingScoreErrorRateByPath = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score_error_rate_by_path",
		Help: "Rolling upstream error rate by normalized Beacon API path",
	}, []string{"network", "upstream", "api_path"})

	metricRoutingScoreP90LatencyByPath = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score_p90_latency_seconds_by_path",
		Help: "Rolling upstream p90 latency by normalized Beacon API path",
	}, []string{"network", "upstream", "api_path"})

	metricRoutingScoreSamplesByPath = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_score_samples_by_path",
		Help: "Rolling sample count by normalized Beacon API path used for upstream score computation",
	}, []string{"network", "upstream", "api_path"})

	metricFinalizedEpoch = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_finalized_epoch",
		Help: "Latest observed finalized epoch for each upstream",
	}, []string{"network", "upstream"})

	metricArchive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ebeacon_upstream_archive",
		Help: "Whether upstream is configured as archive (1=archive, 0=pruned)",
	}, []string{"network", "upstream"})
)

// Known consensus client types (auto-detected via /eth/v1/node/version).
const (
	ClientUnknown    = "unknown"
	ClientLighthouse = "lighthouse"
	ClientPrysm      = "prysm"
	ClientTeku       = "teku"
	ClientNimbus     = "nimbus"
	ClientLodestar   = "lodestar"
	ClientGrandine   = "grandine"
	ClientCaplin     = "caplin"
)

// Upstream represents a single beacon node endpoint.
type Upstream struct {
	ID        string
	NetworkID string
	URL       string
	Headers   map[string]string
	Client    *http.Client
	Priority  int
	Weight    int
	Archive   bool

	// Health state
	mu             sync.RWMutex
	health         HealthStatus
	nodeVerdict    HealthStatus
	syncVerdict    HealthStatus
	headSlot       uint64
	headRoot       string
	syncDistance   uint64
	isSyncing      bool
	finalizedEpoch uint64
	clientType     string

	// Circuit breaker
	cbMu      sync.Mutex
	cbState   CBState
	cbFails   int
	cbSuccess int
	cbLastErr time.Time
	cbCfg     config.CircuitBreakerConfig

	// Concurrency tracking
	activeConns atomic.Int64

	// Score tracking
	scorer       *ScoreTracker
	routeScoreMu sync.RWMutex
	routeScorers map[string]*ScoreTracker

	// Per-upstream rate limit auto-tuner
	rateLimiter *AutoTuner
}

const minPathScopedScoreSamples = 5

// New creates an Upstream for the given network and upstream config.
func New(networkID string, cfg config.UpstreamConfig, defaultCB *config.CircuitBreakerConfig, scoreWindowSize int) *Upstream {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     200,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	u := &Upstream{
		ID:           cfg.ID,
		NetworkID:    networkID,
		URL:          strings.TrimRight(cfg.URL, "/"),
		Headers:      cfg.Headers,
		Client:       &http.Client{Transport: transport},
		Priority:     cfg.Priority,
		Weight:       cfg.Weight,
		Archive:      cfg.Archive,
		health:       HealthUp,
		nodeVerdict:  HealthUp,
		syncVerdict:  HealthUp,
		clientType:   ClientUnknown,
		scorer:       NewScoreTracker(scoreWindowSize),
		routeScorers: make(map[string]*ScoreTracker),
	}

	if cfg.RateLimiting != nil && cfg.RateLimiting.AutoTune {
		u.rateLimiter = NewAutoTuner(
			cfg.RateLimiting.InitialRate,
			cfg.RateLimiting.MinRate,
			cfg.RateLimiting.MaxRate,
			cfg.RateLimiting.AdjustmentPeriod,
			cfg.RateLimiting.ErrorRateThreshold,
		)
	}

	u.cbCfg = config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	if defaultCB != nil {
		u.cbCfg = *defaultCB
	}
	if cfg.Failsafe != nil && cfg.Failsafe.CircuitBreaker != nil {
		override := cfg.Failsafe.CircuitBreaker
		if override.FailureThreshold != 0 {
			u.cbCfg.FailureThreshold = override.FailureThreshold
		}
		if override.SuccessThreshold != 0 {
			u.cbCfg.SuccessThreshold = override.SuccessThreshold
		}
		if override.HalfOpenAfter != 0 {
			u.cbCfg.HalfOpenAfter = override.HalfOpenAfter
		}
	}

	metricHealth.WithLabelValues(networkID, u.ID).Set(float64(HealthUp))
	metricCBState.WithLabelValues(networkID, u.ID).Set(0)
	metricActiveConns.WithLabelValues(networkID, u.ID).Set(0)
	metricHeadSlot.WithLabelValues(networkID, u.ID).Set(0)
	metricSyncDistance.WithLabelValues(networkID, u.ID).Set(0)
	metricRoutingScore.WithLabelValues(networkID, u.ID).Set(0)
	metricRoutingScoreErrorRate.WithLabelValues(networkID, u.ID).Set(0)
	metricRoutingScoreP90Latency.WithLabelValues(networkID, u.ID).Set(0)
	metricRoutingScoreHeadLag.WithLabelValues(networkID, u.ID).Set(0)
	metricRoutingScoreSamples.WithLabelValues(networkID, u.ID).Set(0)
	metricFinalizedEpoch.WithLabelValues(networkID, u.ID).Set(0)
	if cfg.Archive {
		metricArchive.WithLabelValues(networkID, u.ID).Set(1)
	} else {
		metricArchive.WithLabelValues(networkID, u.ID).Set(0)
	}

	return u
}

// IsReady returns true if the upstream is healthy and the circuit breaker allows requests.
func (u *Upstream) IsReady() bool {
	return u.IsHealthy() && u.cbAllow()
}

// IsArchive returns true if the upstream is explicitly marked as archive-capable.
// Archive upstreams are expected to serve historical data that pruned nodes may
// have already pruned (old blocks, old states, old blob sidecars).
func (u *Upstream) IsArchive() bool {
	return u.Archive
}

// IsHealthy returns true if the upstream is reachable (up or degraded).
func (u *Upstream) IsHealthy() bool {
	u.mu.RLock()
	h := u.health
	u.mu.RUnlock()
	return h != HealthDown
}

// Health returns the upstream's current health status.
func (u *Upstream) Health() HealthStatus {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.health
}

// recomputeHealthLocked derives health = min(nodeVerdict, syncVerdict). The
// two pollers run independently; deriving from per-source verdicts under one
// lock is what stops a 206 node-health verdict and a healthy /syncing
// response from flapping the shared state.
func (u *Upstream) recomputeHealthLocked() (old, h HealthStatus) {
	old = u.health
	u.health = min(u.nodeVerdict, u.syncVerdict)
	metricHealth.WithLabelValues(u.NetworkID, u.ID).Set(float64(u.health))
	return old, u.health
}

func (u *Upstream) logHealthTransition(old, h HealthStatus) {
	if old != h {
		slog.Info("upstream health changed",
			"network", u.NetworkID, "upstream", u.ID,
			"from", healthName(old), "to", healthName(h))
	}
}

// SetHealth overrides health directly: both probe verdicts reset to h, so
// Health() returns exactly h until the next probe adjusts a verdict.
func (u *Upstream) SetHealth(h HealthStatus) {
	u.mu.Lock()
	u.nodeVerdict = h
	u.syncVerdict = h
	old, h := u.recomputeHealthLocked()
	u.mu.Unlock()
	u.logHealthTransition(old, h)
}

func (u *Upstream) setNodeVerdict(v HealthStatus) {
	u.mu.Lock()
	u.nodeVerdict = v
	old, h := u.recomputeHealthLocked()
	u.mu.Unlock()
	u.logHealthTransition(old, h)
}

func (u *Upstream) setSyncVerdict(v HealthStatus) {
	u.mu.Lock()
	u.syncVerdict = v
	old, h := u.recomputeHealthLocked()
	u.mu.Unlock()
	u.logHealthTransition(old, h)
}

// UpdateSyncStatus records the latest sync status.
func (u *Upstream) UpdateSyncStatus(isSyncing bool, headSlot, syncDistance uint64) {
	u.mu.Lock()
	u.isSyncing = isSyncing
	u.headSlot = headSlot
	u.syncDistance = syncDistance
	if isSyncing {
		u.syncVerdict = HealthDegraded
	} else {
		u.syncVerdict = HealthUp
	}
	old, h := u.recomputeHealthLocked()
	u.mu.Unlock()
	u.logHealthTransition(old, h)
	metricHeadSlot.WithLabelValues(u.NetworkID, u.ID).Set(float64(headSlot))
	metricSyncDistance.WithLabelValues(u.NetworkID, u.ID).Set(float64(syncDistance))
}

// UpdateFinalizedEpoch records the latest finalized epoch for this upstream.
func (u *Upstream) UpdateFinalizedEpoch(epoch uint64) {
	u.mu.Lock()
	if epoch > u.finalizedEpoch {
		u.finalizedEpoch = epoch
		metricFinalizedEpoch.WithLabelValues(u.NetworkID, u.ID).Set(float64(epoch))
	}
	u.mu.Unlock()
}

// FinalizedEpoch returns the last known finalized epoch.
func (u *Upstream) FinalizedEpoch() uint64 {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.finalizedEpoch
}

// HeadSlot returns the last known head slot.
func (u *Upstream) HeadSlot() uint64 {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.headSlot
}

// HeadRoot returns the last known head block root.
func (u *Upstream) HeadRoot() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.headRoot
}

// UpdateHeadBlock records the latest head block information.
func (u *Upstream) UpdateHeadBlock(slot uint64, root, parentRoot string) {
	u.mu.Lock()
	u.headSlot = slot
	u.headRoot = root
	u.mu.Unlock()
	metricHeadSlot.WithLabelValues(u.NetworkID, u.ID).Set(float64(slot))
}

// ClientType returns the auto-detected consensus client type.
func (u *Upstream) ClientType() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.clientType
}

// SetClientType sets the detected consensus client type.
func (u *Upstream) SetClientType(ct string) {
	u.mu.Lock()
	old := u.clientType
	u.clientType = ct
	u.mu.Unlock()
	if old != ct {
		slog.Info("upstream client type detected",
			"network", u.NetworkID, "upstream", u.ID, "type", ct)
	}
}

// SyncDistance returns the last known sync distance.
func (u *Upstream) SyncDistance() uint64 {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.syncDistance
}

// Score computes a composite quality score with the given weights and head lag.
func (u *Upstream) Score(errorW, latencyW, headLagW, syncDistW float64, canonicalSlot uint64) float64 {
	headSlot := u.HeadSlot()
	var headLag uint64
	if canonicalSlot > headSlot {
		headLag = canonicalSlot - headSlot
	}
	return u.scorer.Score(errorW, latencyW, headLagW, syncDistW, headLag, u.SyncDistance())
}

// ScoreSnapshot returns the rolling score inputs for this upstream.
func (u *Upstream) ScoreSnapshot() ScoreSnapshot {
	return u.scorer.Snapshot()
}

// PathScoreSnapshot returns the raw route-scoped score inputs for this upstream.
func (u *Upstream) PathScoreSnapshot(apiPath string) ScoreSnapshot {
	if apiPath == "" {
		return u.scorer.Snapshot()
	}
	scorer := u.routeScorer(apiPath, false)
	if scorer == nil {
		return ScoreSnapshot{}
	}
	return scorer.Snapshot()
}

// ScoreSnapshotForPath returns a route-scoped snapshot when it has enough
// samples to be representative; otherwise it falls back to the global tracker.
func (u *Upstream) ScoreSnapshotForPath(apiPath string) ScoreSnapshot {
	if apiPath == "" {
		return u.scorer.Snapshot()
	}
	snapshot := u.PathScoreSnapshot(apiPath)
	if snapshot.Samples < minPathScopedScoreSamples {
		return u.scorer.Snapshot()
	}
	return snapshot
}

// RecordSuccess records a successful request with its duration for scoring.
func (u *Upstream) RecordSuccess(d time.Duration) {
	u.RecordSuccessForPath("", d)
}

// RecordSuccessForPath records a successful request against both the global
// tracker and the normalized API path tracker.
func (u *Upstream) RecordSuccessForPath(apiPath string, d time.Duration) {
	u.scorer.RecordSuccess(d)
	if apiPath == "" {
		return
	}
	u.routeScorer(apiPath, true).RecordSuccess(d)
}

// RecordScoreError records a failed request for scoring.
func (u *Upstream) RecordScoreError() {
	u.RecordScoreErrorForPath("")
}

// RecordScoreErrorForPath records a failed request against both the global
// tracker and the normalized API path tracker.
func (u *Upstream) RecordScoreErrorForPath(apiPath string) {
	u.scorer.RecordError()
	if apiPath == "" {
		return
	}
	u.routeScorer(apiPath, true).RecordError()
}

// AllowRequest is a non-consuming capacity check against the auto-tuner rate
// limiter (returns true if no tuner configured). Tokens are consumed by
// ConsumeRateToken when a request is actually sent.
func (u *Upstream) AllowRequest() bool {
	if u.rateLimiter == nil {
		return true
	}
	return u.rateLimiter.HasCapacity()
}

// ConsumeRateToken consumes one auto-tuner token for a request actually sent.
func (u *Upstream) ConsumeRateToken() {
	if u.rateLimiter != nil {
		u.rateLimiter.Consume()
	}
}

// RecordResponseStatus feeds the auto-tuner with response status codes.
func (u *Upstream) RecordResponseStatus(statusCode int) {
	if u.rateLimiter != nil {
		u.rateLimiter.RecordResponse(statusCode)
	}
}

// ActiveConns returns the number of in-flight requests.
func (u *Upstream) ActiveConns() int64 { return u.activeConns.Load() }

// IncrActive increments the active connection counter.
func (u *Upstream) IncrActive() {
	v := u.activeConns.Add(1)
	metricActiveConns.WithLabelValues(u.NetworkID, u.ID).Set(float64(v))
	metricTotalRequests.WithLabelValues(u.NetworkID, u.ID).Inc()
}

// DecrActive decrements the active connection counter.
func (u *Upstream) DecrActive() {
	v := u.activeConns.Add(-1)
	metricActiveConns.WithLabelValues(u.NetworkID, u.ID).Set(float64(v))
}

// TrackResponse keeps active connection accounting accurate until the response
// body is fully consumed or explicitly closed.
func (u *Upstream) TrackResponse(resp *http.Response) *http.Response {
	if resp == nil || resp.Body == nil {
		u.DecrActive()
		return resp
	}
	resp.Body = &trackedBody{
		ReadCloser: resp.Body,
		done:       u.DecrActive,
	}
	return resp
}

// RecordError increments the error counter.
func (u *Upstream) RecordError() {
	metricTotalErrors.WithLabelValues(u.NetworkID, u.ID).Inc()
}

func (u *Upstream) routeScorer(apiPath string, create bool) *ScoreTracker {
	u.routeScoreMu.RLock()
	scorer := u.routeScorers[apiPath]
	u.routeScoreMu.RUnlock()
	if scorer != nil || !create || apiPath == "" {
		return scorer
	}

	u.routeScoreMu.Lock()
	defer u.routeScoreMu.Unlock()
	if scorer = u.routeScorers[apiPath]; scorer != nil {
		return scorer
	}
	scorer = NewScoreTracker(u.scorer.windowSize)
	u.routeScorers[apiPath] = scorer
	return scorer
}

// CBSuccess records a successful request; may close an open/half-open circuit.
func (u *Upstream) CBSuccess() {
	u.cbMu.Lock()
	defer u.cbMu.Unlock()
	switch u.cbState {
	case CBHalfOpen:
		u.cbSuccess++
		if u.cbSuccess >= u.cbCfg.SuccessThreshold {
			u.cbState = CBClosed
			u.cbFails = 0
			slog.Info("circuit breaker closed", "network", u.NetworkID, "upstream", u.ID)
		}
	case CBClosed:
		u.cbFails = 0
	}
	metricCBState.WithLabelValues(u.NetworkID, u.ID).Set(float64(u.cbState))
}

// CBFailure records a failed request; may open the circuit.
func (u *Upstream) CBFailure() {
	u.cbMu.Lock()
	defer u.cbMu.Unlock()
	u.cbFails++
	u.cbLastErr = time.Now()
	if u.cbState == CBHalfOpen || u.cbFails >= u.cbCfg.FailureThreshold {
		if u.cbState != CBOpen {
			slog.Warn("circuit breaker opened",
				"network", u.NetworkID, "upstream", u.ID, "failures", u.cbFails)
		}
		u.cbState = CBOpen
		u.cbSuccess = 0
	}
	metricCBState.WithLabelValues(u.NetworkID, u.ID).Set(float64(u.cbState))
}

func (u *Upstream) cbAllow() bool {
	u.cbMu.Lock()
	defer u.cbMu.Unlock()
	switch u.cbState {
	case CBClosed:
		return true
	case CBOpen:
		if time.Since(u.cbLastErr) >= u.cbCfg.HalfOpenAfter {
			u.cbState = CBHalfOpen
			u.cbSuccess = 0
			slog.Info("circuit breaker half-open", "network", u.NetworkID, "upstream", u.ID)
			metricCBState.WithLabelValues(u.NetworkID, u.ID).Set(float64(CBHalfOpen))
			return true
		}
		return false
	case CBHalfOpen:
		return true
	}
	return true
}

func healthName(h HealthStatus) string {
	switch h {
	case HealthDown:
		return "down"
	case HealthDegraded:
		return "degraded"
	case HealthUp:
		return "up"
	}
	return "unknown"
}

type trackedBody struct {
	io.ReadCloser
	once sync.Once
	done func()
}

func (b *trackedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
}
