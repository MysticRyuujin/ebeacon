package cache

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
)

var cacheLabelSeq atomic.Uint64

func netLabel(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test_%d_%s", cacheLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_"))
}

func TestCache_Policy_DefaultGenesis(t *testing.T) {
	t.Parallel()
	c, err := New(netLabel(t), config.CacheConfig{Enabled: true, MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	p := c.Policy(http.MethodGet, "/eth/v1/beacon/genesis")
	if p == nil {
		t.Fatal("expected policy for genesis")
	}
	if p.TTL() != 0 {
		t.Fatalf("genesis TTL: got %v", p.TTL())
	}
}

func TestCache_Policy_DefaultHotPaths(t *testing.T) {
	t.Parallel()
	c, err := New(netLabel(t), config.CacheConfig{Enabled: true, MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		path string
		ttl  time.Duration
	}{
		{path: "/eth/v1/node/identity", ttl: time.Hour},
		// Named state IDs get 4 s (slot-boundary aligned when genesisTime is set).
		{path: "/eth/v1/beacon/states/head/validators", ttl: 4 * time.Second},
		{path: "/eth/v1/beacon/states/head/validators/12345", ttl: 4 * time.Second},
		{path: "/eth/v1/beacon/states/head/pending_deposits", ttl: 4 * time.Second},
		// Numeric slot IDs keep the 12 s TTL and are finality-promoted.
		{path: "/eth/v1/beacon/blobs/12345", ttl: 12 * time.Second},
		{path: "/eth/v1/beacon/blob_sidecars/12345", ttl: 12 * time.Second},
		{path: "/eth/v1/beacon/rewards/blocks/12345", ttl: 12 * time.Second},
		{path: "/eth/v1/beacon/rewards/sync_committee/12345", ttl: 12 * time.Second},
		{path: "/eth/v1/beacon/rewards/attestations/123", ttl: 30 * time.Second},
		{path: "/eth/v1/validator/duties/proposer/123", ttl: 30 * time.Second},
	}

	for _, tt := range tests {
		p := c.Policy(http.MethodGet, tt.path)
		if p == nil {
			t.Fatalf("expected policy for %s", tt.path)
		}
		if got := p.TTL(); got != tt.ttl {
			t.Fatalf("TTL for %s: got %v want %v", tt.path, got, tt.ttl)
		}
	}
}

func TestCache_Policy_DefaultIntentionalMisses(t *testing.T) {
	t.Parallel()
	c, err := New(netLabel(t), config.CacheConfig{Enabled: true, MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"/eth/v1/config/spec", "/eth/v1/node/health"} {
		if p := c.Policy(http.MethodGet, path); p != nil {
			t.Fatalf("expected no default policy for %s", path)
		}
	}
}

func TestCache_GetSet(t *testing.T) {
	t.Parallel()
	c, err := New(netLabel(t), config.CacheConfig{Enabled: true, MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	c.Set("k1", 200, h, []byte(`{}`), time.Minute)
	e := c.Get("k1")
	if e == nil {
		t.Fatal("expected entry")
	}
	if e.Status() != 200 {
		t.Fatalf("status: %d", e.Status())
	}
}

func TestCache_EntryExpiresAfterTTL(t *testing.T) {
	t.Parallel()
	c, err := New(netLabel(t), config.CacheConfig{Enabled: true, MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	c.Set("ttl-key", 200, h, []byte(`{}`), 20*time.Millisecond)

	time.Sleep(35 * time.Millisecond)

	if got := c.Get("ttl-key"); got != nil {
		t.Fatal("expected expired entry to be evicted")
	}
}

func TestCache_PromotePreventsExpiry(t *testing.T) {
	t.Parallel()
	c, err := New(netLabel(t), config.CacheConfig{Enabled: true, MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	c.Set("promote-key", 200, h, []byte(`{}`), 20*time.Millisecond)
	c.Promote("promote-key")

	time.Sleep(35 * time.Millisecond)

	if got := c.Get("promote-key"); got == nil {
		t.Fatal("expected promoted entry to survive original TTL")
	}
}

func TestMemoryStore_EvictsLeastRecentlyUsed(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore(2)
	now := time.Now()

	store.Set("k1", &Entry{key: "k1", status: 200, created: now}, time.Minute)
	store.Set("k2", &Entry{key: "k2", status: 200, created: now}, time.Minute)

	// Touch k1 so k2 becomes LRU.
	if _, ok := store.Get("k1"); !ok {
		t.Fatal("expected k1 in store")
	}

	store.Set("k3", &Entry{key: "k3", status: 200, created: now}, time.Minute)

	if _, ok := store.Get("k2"); ok {
		t.Fatal("expected k2 to be evicted as LRU")
	}
	if _, ok := store.Get("k1"); !ok {
		t.Fatal("expected k1 to remain after LRU eviction")
	}
	if _, ok := store.Get("k3"); !ok {
		t.Fatal("expected k3 to be present")
	}
}

func TestMemoryStore_EntriesReturnsNewestFirstAndSkipsExpired(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore(4)
	now := time.Now()

	store.Set("fresh-1", &Entry{key: "fresh-1", status: 200, created: now}, time.Minute)
	store.Set("expired", &Entry{key: "expired", status: 200, created: now.Add(time.Second), expires: now.Add(-time.Second)}, time.Minute)
	store.Set("fresh-2", &Entry{key: "fresh-2", status: 200, created: now.Add(2 * time.Second)}, time.Minute)

	entries := store.Entries(10, false)
	if len(entries) != 2 {
		t.Fatalf("entries len: got %d want 2", len(entries))
	}
	if entries[0].key != "fresh-2" {
		t.Fatalf("first entry key: got %q want %q", entries[0].key, "fresh-2")
	}
	if entries[1].key != "fresh-1" {
		t.Fatalf("second entry key: got %q want %q", entries[1].key, "fresh-1")
	}
}

func TestCache_EntriesSkipsBodyUnlessRequested(t *testing.T) {
	t.Parallel()
	c, err := New(netLabel(t), config.CacheConfig{Enabled: true, MaxSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"data":"cached"}`)
	c.Set("k1", http.StatusOK, http.Header{"Content-Type": {"application/json"}}, body, time.Minute)

	withoutBody := c.Entries(10, false)
	if len(withoutBody) != 1 {
		t.Fatalf("entries len without body: got %d want 1", len(withoutBody))
	}
	if withoutBody[0].Body != nil {
		t.Fatal("expected body to be omitted by default")
	}
	if withoutBody[0].BodySize != len(body) {
		t.Fatalf("body size without body: got %d want %d", withoutBody[0].BodySize, len(body))
	}

	withBody := c.Entries(10, true)
	if len(withBody) != 1 {
		t.Fatalf("entries len with body: got %d want 1", len(withBody))
	}
	if got := string(withBody[0].Body); got != string(body) {
		t.Fatalf("body with includeBody: got %q want %q", got, body)
	}
}
