package cache

import (
	"net/http"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ebeacon/ebeacon/config"
)

func newTestRedisStore(t *testing.T, mr *miniredis.Miniredis, prefix string) *RedisStore {
	t.Helper()
	store, err := NewRedisStore(&config.RedisCacheConfig{
		URL:       "redis://" + mr.Addr(),
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func makeEntry(key string, status int, body []byte) *Entry {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	return &Entry{key: key, status: status, headers: h, body: body, created: time.Now()}
}

func TestRedisStore_GetSet(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "")

	store.Set("k1", makeEntry("k1", 200, []byte(`{}`)), time.Minute)

	got, ok := store.Get("k1")
	if !ok {
		t.Fatal("expected hit")
	}
	if got.Status() != 200 {
		t.Fatalf("status: got %d want 200", got.Status())
	}
}

func TestRedisStore_Miss(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "")

	_, ok := store.Get("missing")
	if ok {
		t.Fatal("expected miss for absent key")
	}
}

func TestRedisStore_Delete(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "")

	store.Set("k1", makeEntry("k1", 200, []byte(`{}`)), time.Minute)
	store.Delete("k1")

	_, ok := store.Get("k1")
	if ok {
		t.Fatal("expected miss after delete")
	}
}

func TestRedisStore_Promote(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "")

	store.Set("k1", makeEntry("k1", 200, []byte(`{}`)), 50*time.Millisecond)
	store.Promote("k1")
	mr.FastForward(100 * time.Millisecond)

	_, ok := store.Get("k1")
	if !ok {
		t.Fatal("expected promoted entry to survive TTL expiry")
	}
}

func TestRedisStore_KeyPrefixIsolation(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	net1 := newTestRedisStore(t, mr, "net1:")
	net2 := newTestRedisStore(t, mr, "net2:")

	net1.Set("k1", makeEntry("k1", 200, []byte(`{}`)), time.Minute)

	if _, ok := net2.Get("k1"); ok {
		t.Fatal("expected prefix isolation: net2 should not see net1 key")
	}
	if _, ok := net1.Get("k1"); !ok {
		t.Fatal("expected net1 key to be present")
	}
}

func TestRedisStore_Entries(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "")

	for _, k := range []string{"a", "b", "c"} {
		store.Set(k, makeEntry(k, 200, []byte(`{}`)), time.Minute)
	}

	entries := store.Entries(10, false)
	if len(entries) != 3 {
		t.Fatalf("entries: got %d want 3", len(entries))
	}
}

func TestRedisStore_EntriesLimit(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	store := newTestRedisStore(t, mr, "")

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		store.Set(k, makeEntry(k, 200, []byte(`{}`)), time.Minute)
	}

	entries := store.Entries(2, false)
	if len(entries) != 2 {
		t.Fatalf("entries with limit 2: got %d", len(entries))
	}
}

func TestRedisStore_Password(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	mr.RequireAuth("s3cr3t")

	_, err := NewRedisStore(&config.RedisCacheConfig{URL: "redis://" + mr.Addr()})
	if err == nil {
		t.Fatal("expected error connecting without password")
	}

	store, err := NewRedisStore(&config.RedisCacheConfig{
		URL:      "redis://" + mr.Addr(),
		Password: "s3cr3t",
	})
	if err != nil {
		t.Fatalf("expected success with correct password: %v", err)
	}

	store.Set("k", makeEntry("k", 200, []byte(`{}`)), time.Minute)
	if _, ok := store.Get("k"); !ok {
		t.Fatal("expected hit after authenticated set")
	}
}
