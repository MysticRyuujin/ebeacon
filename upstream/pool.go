package upstream

import (
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ebeacon/ebeacon/config"
	"github.com/ebeacon/ebeacon/state"
)

// Pool manages a set of upstream beacon nodes for a single network.
type Pool struct {
	networkID string

	mu        sync.RWMutex
	upstreams []*Upstream
	rrCounter atomic.Uint64

	routing    config.RoutingConfig
	monitor    *HealthMonitor
	blockCache *BlockCache

	// finalizedEpoch is the highest finalized epoch seen across all upstreams.
	// Slots at or before epoch*32+31 can be cached forever.
	finalizedEpoch atomic.Uint64

	// sharedState propagates head and finalized updates across instances.
	sharedState state.SharedState

	// lastPublished tracks the most recently published canonical head to avoid
	// redundant publishes when the head has not changed.
	lastPublished struct {
		mu   sync.Mutex
		slot uint64
		root string
	}
}

// UpstreamScoreDetails describes the live score and inputs for one upstream.
type UpstreamScoreDetails struct {
	LoadBalancing string
	Score         float64
	ErrorRate     float64
	P90Latency    time.Duration
	HeadLag       uint64
	SyncDistance  uint64
	Samples       int
}

// NewPool creates a Pool for the given network.
func NewPool(networkID string, upstreamCfgs []config.UpstreamConfig, routing config.RoutingConfig, health config.HealthConfig, defaultCB *config.CircuitBreakerConfig) (*Pool, error) {
	if len(upstreamCfgs) == 0 {
		return nil, fmt.Errorf("at least one upstream is required")
	}

	upstreams := make([]*Upstream, 0, len(upstreamCfgs))
	for _, ucfg := range upstreamCfgs {
		upstreams = append(upstreams, New(networkID, ucfg, defaultCB, routing.ScoreWindowSize))
	}

	if routing.LoadBalancing == "" {
		routing.LoadBalancing = "round-robin"
	}

	p := &Pool{
		networkID:  networkID,
		upstreams:  upstreams,
		routing:    routing,
		blockCache: NewBlockCache(health.FollowDistance, health.MaxHeadDistance),
	}
	p.monitor = newHealthMonitor(upstreams, p, health)
	p.RefreshScoreMetrics()
	return p, nil
}

// BlockCache returns the pool's block cache for fork tracking.
func (p *Pool) BlockCache() *BlockCache {
	return p.blockCache
}

func (p *Pool) effectiveScoreWeights() config.ScoreWeightsConfig {
	weights := config.ScoreWeightsConfig{
		ErrorRate:    4.0,
		Latency:      8.0,
		HeadLag:      2.0,
		SyncDistance: 1.0,
	}
	if p.routing.ScoreWeights == nil {
		return weights
	}
	if p.routing.ScoreWeights.ErrorRate > 0 {
		weights.ErrorRate = p.routing.ScoreWeights.ErrorRate
	}
	if p.routing.ScoreWeights.Latency > 0 {
		weights.Latency = p.routing.ScoreWeights.Latency
	}
	if p.routing.ScoreWeights.HeadLag > 0 {
		weights.HeadLag = p.routing.ScoreWeights.HeadLag
	}
	if p.routing.ScoreWeights.SyncDistance > 0 {
		weights.SyncDistance = p.routing.ScoreWeights.SyncDistance
	}
	return weights
}

func (p *Pool) scoreDetailsWithWeights(u *Upstream, canonicalSlot uint64, weights config.ScoreWeightsConfig) UpstreamScoreDetails {
	if u == nil {
		return UpstreamScoreDetails{LoadBalancing: p.routing.LoadBalancing}
	}
	snapshot := u.ScoreSnapshot()
	headSlot := u.HeadSlot()
	var headLag uint64
	if canonicalSlot > headSlot {
		headLag = canonicalSlot - headSlot
	}
	syncDistance := u.SyncDistance()
	return UpstreamScoreDetails{
		LoadBalancing: p.routing.LoadBalancing,
		Score: calculateScore(
			weights.ErrorRate,
			weights.Latency,
			weights.HeadLag,
			weights.SyncDistance,
			snapshot.ErrorRate,
			snapshot.P90Latency,
			headLag,
			syncDistance,
		),
		ErrorRate:    snapshot.ErrorRate,
		P90Latency:   snapshot.P90Latency,
		HeadLag:      headLag,
		SyncDistance: syncDistance,
		Samples:      snapshot.Samples,
	}
}

func (p *Pool) setScoreMetrics(u *Upstream, details UpstreamScoreDetails) {
	metricRoutingScore.WithLabelValues(p.networkID, u.ID).Set(details.Score)
	metricRoutingScoreErrorRate.WithLabelValues(p.networkID, u.ID).Set(details.ErrorRate)
	metricRoutingScoreP90Latency.WithLabelValues(p.networkID, u.ID).Set(details.P90Latency.Seconds())
	metricRoutingScoreHeadLag.WithLabelValues(p.networkID, u.ID).Set(float64(details.HeadLag))
	metricRoutingScoreSamples.WithLabelValues(p.networkID, u.ID).Set(float64(details.Samples))
}

// ScoreDetails returns the current score snapshot for one upstream.
func (p *Pool) ScoreDetails(u *Upstream) UpstreamScoreDetails {
	return p.scoreDetailsWithWeights(u, p.blockCache.MaxSlot(), p.effectiveScoreWeights())
}

// RefreshUpstreamScoreMetrics updates Prometheus gauges for one upstream.
func (p *Pool) RefreshUpstreamScoreMetrics(u *Upstream) {
	if u == nil {
		return
	}
	details := p.scoreDetailsWithWeights(u, p.blockCache.MaxSlot(), p.effectiveScoreWeights())
	p.setScoreMetrics(u, details)
}

// RefreshScoreMetrics updates Prometheus gauges for all upstreams in the pool.
func (p *Pool) RefreshScoreMetrics() {
	weights := p.effectiveScoreWeights()
	canonicalSlot := p.blockCache.MaxSlot()
	for _, u := range p.All() {
		p.setScoreMetrics(u, p.scoreDetailsWithWeights(u, canonicalSlot, weights))
	}
}

// Start launches background health monitoring.
func (p *Pool) Start(ctx context.Context) {
	p.monitor.start(ctx)
}

// All returns a snapshot of all upstreams.
func (p *Pool) All() []*Upstream {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Upstream, len(p.upstreams))
	copy(out, p.upstreams)
	return out
}

// ByID returns the upstream with the given ID, or nil.
func (p *Pool) ByID(id string) *Upstream {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, u := range p.upstreams {
		if u.ID == id {
			return u
		}
	}
	return nil
}

// Get returns a single upstream, preferring the one matching preferID if ready.
// preferID may be an exact upstream ID or a glob pattern (e.g. *lighthouse*).
func (p *Pool) Get(preferID string) (*Upstream, error) {
	if preferID != "" {
		if strings.ContainsAny(preferID, "*?[") {
			if u := p.ByGlob(preferID); u != nil && u.IsReady() {
				return u, nil
			}
		} else if u := p.ByID(preferID); u != nil && u.IsReady() {
			return u, nil
		}
	}
	ups := p.Select(1)
	if len(ups) == 0 {
		return nil, fmt.Errorf("no upstreams available for network %s", p.networkID)
	}
	return ups[0], nil
}

// SelectByClientType returns up to n upstreams for a single client type,
// ordered by the normal load-balancing policy and limited to that client set.
func (p *Pool) SelectByClientType(clientType string, n int) []*Upstream {
	p.mu.RLock()
	all := make([]*Upstream, 0, len(p.upstreams))
	for _, u := range p.upstreams {
		if u.ClientType() == clientType {
			all = append(all, u)
		}
	}
	p.mu.RUnlock()
	return p.selectFromCandidates(all, n)
}

// SelectByGlob returns up to n upstreams whose IDs match the glob pattern,
// ordered by the normal load-balancing policy and limited to that match set.
func (p *Pool) SelectByGlob(pattern string, n int) []*Upstream {
	p.mu.RLock()
	all := make([]*Upstream, 0, len(p.upstreams))
	for _, u := range p.upstreams {
		if matched, _ := path.Match(pattern, u.ID); matched {
			all = append(all, u)
		}
	}
	p.mu.RUnlock()
	return p.selectFromCandidates(all, n)
}

// ByGlob returns the first ready upstream whose ID matches the glob pattern.
// Pattern syntax follows path.Match: * matches any sequence of non-separator characters.
func (p *Pool) ByGlob(pattern string) *Upstream {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, u := range p.upstreams {
		if matched, _ := path.Match(pattern, u.ID); matched {
			return u
		}
	}
	return nil
}

// Select returns up to n ready upstreams ordered by the load-balancing policy.
// Prefers upstreams on the canonical fork, then falls back to healthy-only, then all.
func (p *Pool) Select(n int) []*Upstream {
	p.mu.RLock()
	all := p.upstreams
	p.mu.RUnlock()
	return p.selectFromCandidates(all, n)
}

func (p *Pool) selectFromCandidates(all []*Upstream, n int) []*Upstream {
	// Prefer upstreams that are both healthy and on the canonical fork.
	// Fall back progressively: canonical+ready → any ready → any healthy → all.
	// The fallback chain ensures we always return something rather than dropping
	// requests during partial outages, at the cost of potentially routing to a
	// lagging or forked upstream as a last resort.
	candidates := filter(all, func(u *Upstream) bool {
		return u.IsReady() && p.blockCache.IsOnCanonicalFork(u.ID)
	})
	if len(candidates) == 0 {
		candidates = filter(all, (*Upstream).IsReady)
	}
	if len(candidates) == 0 {
		candidates = filter(all, (*Upstream).IsHealthy)
	}
	if len(candidates) == 0 {
		candidates = all
	}

	// Prefer upstreams not currently over their auto-tuned rate limit, but
	// fail-open (use all candidates) if every upstream is over-limit to avoid
	// dropping requests during a brief burst.
	allowed := filter(candidates, (*Upstream).AllowRequest)
	if len(allowed) > 0 {
		candidates = allowed
	}

	ordered := p.order(candidates)
	if n > len(ordered) {
		n = len(ordered)
	}
	return ordered[:n]
}

// ByClientType returns the first upstream matching the given client type, or nil.
func (p *Pool) ByClientType(ct string) *Upstream {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, u := range p.upstreams {
		if u.ClientType() == ct {
			return u
		}
	}
	return nil
}

// NodeHealthStatus returns the best health status across all upstreams.
// Used to respond synthetically to /eth/v1/node/health.
func (p *Pool) NodeHealthStatus() HealthStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	best := HealthDown
	for _, u := range p.upstreams {
		if h := u.Health(); h > best {
			best = h
		}
	}
	return best
}

// NodeHealthStatusForSelector returns the best health status for upstreams
// matching selector, which is in the same format as clientUpstream:
// "client:<type>", a glob pattern, or a bare upstream ID.
// Returns HealthDown if no upstreams match.
func (p *Pool) NodeHealthStatusForSelector(selector string) HealthStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	best := HealthDown
	for _, u := range p.upstreams {
		var match bool
		switch {
		case strings.HasPrefix(selector, "client:"):
			match = u.ClientType() == strings.TrimPrefix(selector, "client:")
		case strings.ContainsAny(selector, "*?["):
			match, _ = path.Match(selector, u.ID)
		default:
			match = u.ID == selector
		}
		if match {
			if h := u.Health(); h > best {
				best = h
			}
		}
	}
	return best
}

// HealthCounts returns total, up, degraded, and down counts across all upstreams.
func (p *Pool) HealthCounts() (total, up, degraded, down int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	total = len(p.upstreams)
	for _, u := range p.upstreams {
		switch u.Health() {
		case HealthUp:
			up++
		case HealthDegraded:
			degraded++
		default:
			down++
		}
	}
	return
}

// FinalizedSlot returns the highest finalized slot seen across all upstreams.
// Ethereum's beacon chain organises time into epochs of exactly 32 slots each.
// The last slot of epoch N is epoch*32 + 31. Any block at or before this slot
// is finalized and will never be reorged, so it can be cached indefinitely.
func (p *Pool) FinalizedSlot() uint64 {
	epoch := p.finalizedEpoch.Load()
	if epoch == 0 {
		return 0
	}
	return epoch*32 + 31
}

// UpdateFinalizedEpoch merges a finalized epoch into the pool aggregate and
// publishes it to shared state so other instances learn about it.
func (p *Pool) UpdateFinalizedEpoch(epoch uint64) {
	for {
		old := p.finalizedEpoch.Load()
		if epoch <= old {
			return
		}
		if p.finalizedEpoch.CompareAndSwap(old, epoch) {
			if p.sharedState != nil {
				p.sharedState.PublishFinalized(epoch)
			}
			return
		}
	}
}

// seedFinalizedEpoch updates the pool's finalized epoch without publishing to
// shared state, used when seeding from shared state on startup.
func (p *Pool) seedFinalizedEpoch(epoch uint64) {
	for {
		old := p.finalizedEpoch.Load()
		if epoch <= old {
			return
		}
		if p.finalizedEpoch.CompareAndSwap(old, epoch) {
			return
		}
	}
}

// SetSharedState attaches shared state for cross-instance coordination.
// Must be called before Start.
func (p *Pool) SetSharedState(s state.SharedState) {
	p.sharedState = s
}

// StartStateSync seeds the finalized epoch from shared state (e.g. loaded from
// Redis at startup). Call before Start so health monitoring inherits the value.
func (p *Pool) StartStateSync() {
	if p.sharedState == nil {
		return
	}
	p.seedFinalizedEpoch(p.sharedState.GetFinalized())
}

// SyncCanonicalHead publishes the current canonical head to shared state if it
// changed since the last publish. Called after each head block update.
func (p *Pool) SyncCanonicalHead() {
	if p.sharedState == nil {
		return
	}
	slot, root := p.blockCache.CanonicalHead()
	if slot == 0 || root == "" {
		return
	}
	p.lastPublished.mu.Lock()
	defer p.lastPublished.mu.Unlock()
	if p.lastPublished.slot == slot && p.lastPublished.root == root {
		return
	}
	p.lastPublished.slot = slot
	p.lastPublished.root = root
	p.sharedState.PublishHead(p.networkID, slot, root)
}

// RecordRemoteHead records a canonical head block published by another instance
// so it contributes to local fork detection.
func (p *Pool) RecordRemoteHead(slot uint64, root string) {
	p.blockCache.AddBlock("remote", slot, root, "")
}

// orderRNG is a per-request xorshift64 PRNG used to randomise upstream ordering.
// We avoid math/rand here because its global source requires a mutex, which
// would become a contention point at high request rates. The xorshift sequence
// is seeded from the monotonically incrementing round-robin counter, so each
// request gets a different but fully deterministic shuffle without any locking.
type orderRNG struct {
	state uint64
}

func newOrderRNG(seed uint64) *orderRNG {
	if seed == 0 {
		seed = 1 // xorshift64 must not be seeded with zero
	}
	return &orderRNG{state: seed}
}

func (r *orderRNG) next() uint64 {
	x := r.state
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	r.state = x
	return x
}

func (r *orderRNG) Float64() float64 {
	return float64(r.next()>>11) / float64(uint64(1)<<53)
}

func upstreamWeight(u *Upstream) int {
	if u == nil || u.Weight <= 0 {
		return 1
	}
	return u.Weight
}

func weightedPrimaryIndex(ups []*Upstream, index uint64) int {
	if len(ups) == 0 {
		return -1
	}
	totalWeight := 0
	for _, u := range ups {
		totalWeight += upstreamWeight(u)
	}
	if totalWeight <= 0 {
		return int(index % uint64(len(ups)))
	}
	target := int(index % uint64(totalWeight))
	cumulative := 0
	for idx, u := range ups {
		cumulative += upstreamWeight(u)
		if target < cumulative {
			return idx
		}
	}
	return len(ups) - 1
}

func rotateUniqueFromIndex(ups []*Upstream, first int) []*Upstream {
	if len(ups) == 0 {
		return nil
	}
	if len(ups) == 1 {
		return []*Upstream{ups[0]}
	}
	if first < 0 || first >= len(ups) {
		first = 0
	}
	out := make([]*Upstream, 0, len(ups))
	out = append(out, ups[first])
	for offset := 1; offset < len(ups); offset++ {
		out = append(out, ups[(first+offset)%len(ups)])
	}
	return out
}

func weightedOrderWithoutReplacement(ups []*Upstream, rng *orderRNG, baseWeight func(*Upstream) float64) []*Upstream {
	remaining := make([]*Upstream, len(ups))
	copy(remaining, ups)
	ordered := make([]*Upstream, 0, len(ups))

	for len(remaining) > 0 {
		weights := make([]float64, len(remaining))
		total := 0.0
		for idx, u := range remaining {
			base := baseWeight(u)
			if math.IsNaN(base) || math.IsInf(base, 0) || base < 0 {
				base = 0
			}
			weight := base * float64(upstreamWeight(u))
			weights[idx] = weight
			total += weight
		}
		if total <= 0 {
			total = 0
			for idx, u := range remaining {
				weights[idx] = float64(upstreamWeight(u))
				total += weights[idx]
			}
		}

		target := rng.Float64() * total
		cumulative := 0.0
		chosen := len(remaining) - 1
		for idx, weight := range weights {
			cumulative += weight
			if target < cumulative {
				chosen = idx
				break
			}
		}

		ordered = append(ordered, remaining[chosen])
		remaining = append(remaining[:chosen], remaining[chosen+1:]...)
	}

	return ordered
}

func (p *Pool) weightedRoundRobinWithinPriority(ups []*Upstream, index uint64) []*Upstream {
	return rotateUniqueFromIndex(ups, weightedPrimaryIndex(ups, index))
}

func (p *Pool) weightedRandomWithinPriority(ups []*Upstream, rng *orderRNG) []*Upstream {
	return weightedOrderWithoutReplacement(ups, rng, func(*Upstream) float64 { return 1 })
}

func (p *Pool) weightedScoreWithinPriority(ups []*Upstream, canonicalSlot uint64, weights config.ScoreWeightsConfig, rng *orderRNG) []*Upstream {
	return weightedOrderWithoutReplacement(ups, rng, func(u *Upstream) float64 {
		return p.scoreDetailsWithWeights(u, canonicalSlot, weights).Score
	})
}

func (p *Pool) order(ups []*Upstream) []*Upstream {
	out := make([]*Upstream, len(ups))
	copy(out, ups)

	// Sort by priority first (lower = better), then apply LB within tiers
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Priority < out[j].Priority
	})

	requestIndex := p.rrCounter.Add(1) - 1
	rng := newOrderRNG(requestIndex + 1)

	switch p.routing.LoadBalancing {
	case "random":
		p.reorderWithinPriority(out, func(sub []*Upstream) []*Upstream {
			return p.weightedRandomWithinPriority(sub, rng)
		})

	case "least-conn":
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].Priority != out[j].Priority {
				return out[i].Priority < out[j].Priority
			}
			left := float64(out[i].ActiveConns()) / float64(upstreamWeight(out[i]))
			right := float64(out[j].ActiveConns()) / float64(upstreamWeight(out[j]))
			if left != right {
				return left < right
			}
			if out[i].ActiveConns() != out[j].ActiveConns() {
				return out[i].ActiveConns() < out[j].ActiveConns()
			}
			return out[i].ID < out[j].ID
		})

	case "score":
		canonSlot := p.blockCache.MaxSlot()
		weights := p.effectiveScoreWeights()
		p.reorderWithinPriority(out, func(sub []*Upstream) []*Upstream {
			return p.weightedScoreWithinPriority(sub, canonSlot, weights, rng)
		})

	default: // round-robin
		p.reorderWithinPriority(out, func(sub []*Upstream) []*Upstream {
			return p.weightedRoundRobinWithinPriority(sub, requestIndex)
		})
	}

	return out
}

func (p *Pool) reorderWithinPriority(ups []*Upstream, orderFn func([]*Upstream) []*Upstream) {
	i := 0
	for i < len(ups) {
		j := i + 1
		for j < len(ups) && ups[j].Priority == ups[i].Priority {
			j++
		}
		sub := orderFn(ups[i:j])
		copy(ups[i:j], sub)
		i = j
	}
}

func filter(ups []*Upstream, fn func(*Upstream) bool) []*Upstream {
	out := make([]*Upstream, 0, len(ups))
	for _, u := range ups {
		if fn(u) {
			out = append(out, u)
		}
	}
	return out
}
