// Package cache provides a concurrent LRU+TTL response cache with
// finality-aware promotion for beacon chain data.
package cache

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ebeacon/ebeacon/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// defaultPolicies are applied when cache.enabled=true but no policies are configured.
// TTL=0 means cache forever. Entries are matched in order; first match wins.
//
// Finality-aware promotion: effectiveCacheTTL() in network.go promotes any entry
// whose path contains a numeric slot ≤ the current finalized slot to TTL=0
// regardless of the TTL set here, so short TTLs on block/header policies are safe —
// they apply only to head-relative data and automatically become permanent once the
// slot is finalized.
var defaultPolicies = []config.CachePolicy{
	// ── immutable chain constants ────────────────────────────────────────────
	// config/genesis and beacon/genesis are true chain constants, identical across
	// all clients. config/spec, config/fork_schedule, and config/deposit_contract
	// are intentionally not cached: different consensus clients return different
	// field sets or value formats, so requests pass through to the selected upstream.
	{Pattern: `^/eth/v\d+/config/genesis$`, TTL: 0},
	{Pattern: `^/eth/v\d+/beacon/genesis$`, TTL: 0},

	// ── node metadata ────────────────────────────────────────────────────────
	{Pattern: `^/eth/v\d+/node/version$`, TTL: time.Hour},
	{Pattern: `^/eth/v\d+/node/identity$`, TTL: time.Hour},
	{Pattern: `^/eth/v\d+/node/peer_count$`, TTL: 15 * time.Second},
	{Pattern: `^/eth/v\d+/node/peers`, TTL: 30 * time.Second},
	{Pattern: `^/eth/v\d+/node/syncing$`, TTL: 5 * time.Second},
	// node/health is intentionally uncached so liveness checks remain fresh.

	// ── beacon blocks & headers ──────────────────────────────────────────────
	// Named block_ids (head, finalized, justified) use a short TTL that
	// effectiveCacheTTL() further shrinks to the remaining time in the current
	// Ethereum slot when genesisTime is configured on the network — ensuring
	// cached head data never crosses a slot boundary.
	//
	// Numeric slot IDs keep the full 12 s TTL and are auto-promoted to TTL=0
	// by effectiveCacheTTL() once the slot is finalized.
	{Pattern: `^/eth/v\d+/beacon/headers$`, TTL: 4 * time.Second},                                 // bare /headers = head
	{Pattern: `^/eth/v\d+/beacon/headers/(head|finalized|justified)$`, TTL: 4 * time.Second},      // named
	{Pattern: `^/eth/v\d+/beacon/headers/`, TTL: 12 * time.Second},                                // numeric (finality-promoted)
	{Pattern: `^/eth/v\d+/beacon/blocks/(head|finalized|justified)(/.*)?$`, TTL: 4 * time.Second}, // named
	{Pattern: `^/eth/v\d+/beacon/blocks/[^/]+/root$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/blocks/[^/]+/attestations$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/blocks/`, TTL: 12 * time.Second},                                         // numeric (finality-promoted)
	{Pattern: `^/eth/v\d+/beacon/blinded_blocks/(head|finalized|justified)(/.*)?$`, TTL: 4 * time.Second}, // named
	{Pattern: `^/eth/v\d+/beacon/blinded_blocks/`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/blobs/(head|finalized|justified)(/.*)?$`, TTL: 4 * time.Second}, // named
	{Pattern: `^/eth/v\d+/beacon/blobs/`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/blob_sidecars/(head|finalized|justified)(/.*)?$`, TTL: 4 * time.Second}, // named
	{Pattern: `^/eth/v\d+/beacon/blob_sidecars/`, TTL: 12 * time.Second},

	// ── beacon rewards ───────────────────────────────────────────────────────
	// Block-scoped rewards become immutable once the referenced slot finalizes.
	// Epoch-scoped rewards are promoted to TTL=0 once the epoch itself is finalized.
	{Pattern: `^/eth/v\d+/beacon/rewards/blocks/[^/]+$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/rewards/sync_committee/[^/]+$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/rewards/attestations/[^/]+$`, TTL: 30 * time.Second},

	// ── beacon state sub-resources ───────────────────────────────────────────
	// Named state IDs (head, finalized, justified) get a short TTL subject to
	// slot-boundary alignment. Numeric state IDs get 12 s and are finality-promoted.
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/root$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/root$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/fork$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/fork$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/finality_checkpoints$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/finality_checkpoints$`, TTL: 12 * time.Second},
	// Validator lookups dominate public read traffic; keep head-relative data short
	// while finalized numeric state_ids auto-promote to permanent entries.
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/validators(?:/[^/]+)?$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/validators(?:/[^/]+)?$`, TTL: 6 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/validator_balances$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/validator_balances$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/committees$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/committees$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/sync_committees$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/sync_committees$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/pending_deposits$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/pending_deposits$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/pending_consolidations$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/pending_consolidations$`, TTL: 12 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/(head|finalized|justified)/pending_partial_withdrawals$`, TTL: 4 * time.Second},
	{Pattern: `^/eth/v\d+/beacon/states/[^/]+/pending_partial_withdrawals$`, TTL: 12 * time.Second},

	// ── validator duties ──────────────────────────────────────────────────────
	// Proposer duties are epoch-scoped and become immutable once the epoch finalizes.
	{Pattern: `^/eth/v\d+/validator/duties/proposer/[^/]+$`, TTL: 30 * time.Second},
}

// Entry holds a cached HTTP response.
type Entry struct {
	key     string
	status  int
	headers http.Header
	body    []byte
	created time.Time
	expires time.Time // zero = never expires
}

// Snapshot is a serializable view of a cached response entry.
type Snapshot struct {
	Key      string
	Status   int
	Headers  http.Header
	Body     []byte
	BodySize int
	Created  time.Time
	Expires  time.Time
}

// policy is a compiled cache policy.
type policy struct {
	re      *regexp.Regexp
	ttl     time.Duration
	methods map[string]bool
}

// Cache is a concurrent LRU cache keyed by request signature.
type Cache struct {
	store    Store
	policies []policy

	hits   atomic.Int64
	misses atomic.Int64

	metricHits   prometheus.Counter
	metricMisses prometheus.Counter
	metricSize   prometheus.Gauge
}

// New creates a Cache from the given config. networkID is used for metric labels.
func New(networkID string, cfg config.CacheConfig) (*Cache, error) {
	policies := cfg.Policies
	if len(policies) == 0 {
		policies = defaultPolicies
	}

	compiled := make([]policy, 0, len(policies))
	for _, p := range policies {
		re, err := regexp.Compile(p.Pattern)
		if err != nil {
			return nil, fmt.Errorf("cache policy pattern %q: %w", p.Pattern, err)
		}
		methods := make(map[string]bool)
		for _, m := range p.Methods {
			methods[strings.ToUpper(m)] = true
		}
		if len(methods) == 0 {
			methods["GET"] = true
			methods["HEAD"] = true
		}
		compiled = append(compiled, policy{re: re, ttl: p.TTL, methods: methods})
	}

	var store Store
	switch cfg.Driver {
	case "redis":
		if cfg.Redis == nil {
			return nil, fmt.Errorf("redis config required when driver=redis")
		}
		rs, err := NewRedisStore(cfg.Redis)
		if err != nil {
			return nil, fmt.Errorf("redis cache: %w", err)
		}
		store = rs
	default:
		store = NewMemoryStore(cfg.MaxSize)
	}

	c := &Cache{
		store:    store,
		policies: compiled,
	}

	c.metricHits = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "ebeacon_cache_hits_total",
		Help:        "Cache hits",
		ConstLabels: prometheus.Labels{"network": networkID},
	})
	c.metricMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "ebeacon_cache_misses_total",
		Help:        "Cache misses",
		ConstLabels: prometheus.Labels{"network": networkID},
	})
	c.metricSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "ebeacon_cache_size",
		Help:        "Number of entries currently in cache",
		ConstLabels: prometheus.Labels{"network": networkID},
	})

	return c, nil
}

// Policy returns the matching cache policy for the given method and path, or nil.
func (c *Cache) Policy(method, path string) *policy {
	upper := strings.ToUpper(method)
	for i := range c.policies {
		p := &c.policies[i]
		if !p.methods[upper] {
			continue
		}
		if p.re.MatchString(path) {
			return p
		}
	}
	return nil
}

// Get retrieves a cached entry. Returns nil if not found or expired.
func (c *Cache) Get(key string) *Entry {
	e, ok := c.store.Get(key)
	if !ok {
		c.misses.Add(1)
		c.metricMisses.Inc()
		return nil
	}
	c.hits.Add(1)
	c.metricHits.Inc()
	c.metricSize.Set(float64(c.store.Len()))
	return e
}

// Set stores a response under the given key with the given TTL.
// TTL=0 means the entry never expires.
func (c *Cache) Set(key string, status int, headers http.Header, body []byte, ttl time.Duration) {
	now := time.Now()
	e := &Entry{key: key, status: status, headers: headers.Clone(), body: body, created: now}
	if ttl > 0 {
		e.expires = now.Add(ttl)
	}
	c.store.Set(key, e, ttl)
	c.metricSize.Set(float64(c.store.Len()))
}

// Promote updates the TTL of an existing entry to "forever" (e.g. when its slot becomes finalized).
func (c *Cache) Promote(key string) {
	c.store.Promote(key)
}

// Delete removes an entry from the cache.
func (c *Cache) Delete(key string) {
	c.store.Delete(key)
	c.metricSize.Set(float64(c.store.Len()))
}

// Entries returns up to limit cached entries as snapshots. limit <= 0 means all available entries.
// Bodies are only copied into the snapshots when includeBody is true.
func (c *Cache) Entries(limit int, includeBody bool) []Snapshot {
	entries := c.store.Entries(limit, includeBody)
	out := make([]Snapshot, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		snapshot := Snapshot{
			Key:      entry.key,
			Status:   entry.status,
			Headers:  entry.headers.Clone(),
			BodySize: len(entry.body),
			Created:  entry.created,
			Expires:  entry.expires,
		}
		if includeBody {
			snapshot.Body = entry.body
		}
		out = append(out, snapshot)
	}
	return out
}

// Size returns the number of entries in the cache.
func (c *Cache) Size() int {
	return c.store.Len()
}

// WriteTo writes the cached entry to an http.ResponseWriter.
func (e *Entry) WriteTo(w http.ResponseWriter) {
	copyHeaders(w.Header(), e.headers)
	w.Header().Set("X-Ebeacon-Cache", "HIT")
	w.WriteHeader(e.status)
	w.Write(e.body) //nolint:errcheck
}

// Status returns the cached HTTP status.
func (e *Entry) Status() int { return e.status }

// Body returns the cached body bytes.
func (e *Entry) Body() []byte { return e.body }

// Headers returns the cached response headers.
func (e *Entry) Headers() http.Header { return e.headers }

func cloneEntry(e *Entry, includeBody bool) *Entry {
	if e == nil {
		return nil
	}
	body := e.body
	if includeBody {
		body = append([]byte(nil), e.body...)
	}
	return &Entry{
		key:     e.key,
		status:  e.status,
		headers: e.headers.Clone(),
		body:    body,
		created: e.created,
		expires: e.expires,
	}
}

// TTL returns how long this policy caches responses.
func (p *policy) TTL() time.Duration { return p.ttl }

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
