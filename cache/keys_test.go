package cache

import (
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestMemoryStore_KeysSkipsExpiredAndKeepsLRUOrder(t *testing.T) {
	t.Parallel()
	m := NewMemoryStore(10)
	m.Set("oldest", makeEntry("oldest", 200, []byte(`{}`)), 0)
	expired := makeEntry("expired", 200, []byte(`{}`))
	expired.expires = time.Now().Add(-time.Second)
	m.Set("expired", expired, time.Nanosecond)
	m.Set("newest", makeEntry("newest", 200, []byte(`{}`)), 0)

	keys := m.Keys()
	want := []string{"newest", "oldest"}
	if !slices.Equal(keys, want) {
		t.Fatalf("Keys: got %v want %v (newest-first, expired removed)", keys, want)
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("expired entry not removed during Keys: len %d want 2", got)
	}
}

func TestRedisStore_KeysStripsPrefix(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "ebeacon:testnet:")
	store.Set("k1", makeEntry("k1", 200, []byte(`{}`)), time.Minute)
	store.Set("k2", makeEntry("k2", 200, []byte(`{}`)), time.Minute)

	keys := store.Keys()
	slices.Sort(keys)
	want := []string{"k1", "k2"}
	if !slices.Equal(keys, want) {
		t.Fatalf("Keys: got %v want %v", keys, want)
	}
}

func TestRedisStore_KeysGlobMetacharPrefixDoesNotPanic(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "x*")
	store.Set("k1", makeEntry("k1", 200, []byte(`{}`)), time.Minute)
	mr.Set("x", "unrelated") //nolint:errcheck

	keys := store.Keys()
	if !slices.Equal(keys, []string{"k1"}) {
		t.Fatalf("Keys with glob-metachar prefix: got %v want [k1]", keys)
	}
}

func TestRedisStore_LenServesCachedCount(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "")
	store.Set("k1", makeEntry("k1", 200, []byte(`{}`)), time.Minute)
	store.Set("k2", makeEntry("k2", 200, []byte(`{}`)), time.Minute)

	if got := store.Len(); got != 2 {
		t.Fatalf("Len: got %d want 2", got)
	}

	// Within lenCacheTTL the cached count is served without a new SCAN, so
	// the value is intentionally stale; a Redis error must also return the
	// last known count instead of zeroing the gauge.
	store.Set("k3", makeEntry("k3", 200, []byte(`{}`)), time.Minute)
	if got := store.Len(); got != 2 {
		t.Fatalf("Len should serve cached count inside the TTL window: got %d want 2", got)
	}
	store.countAt = time.Time{}
	mr.SetError("redis unavailable")
	if got := store.Len(); got != 2 {
		t.Fatalf("Len should return last known count on error: got %d want 2", got)
	}
	mr.SetError("")
	store.countAt = time.Time{}
	if got := store.Len(); got != 3 {
		t.Fatalf("Len after cache expiry: got %d want 3", got)
	}
}
