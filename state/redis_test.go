package state

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ebeacon/ebeacon/config"
)

func newTestRedisState(t *testing.T, mr *miniredis.Miniredis) *RedisState {
	t.Helper()
	rs, err := NewRedisState(&config.RedisStateConfig{
		URL: "redis://" + mr.Addr(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rs.Close() }) //nolint:errcheck
	return rs
}

func TestRedisState_PublishSubscribeHead(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	subscriber := newTestRedisState(t, mr)
	// Allow subscribe goroutine to establish before publishing.
	time.Sleep(50 * time.Millisecond)

	publisher := newTestRedisState(t, mr)
	publisher.PublishHead("hoodi", 42, "0xdeadbeef")

	select {
	case up := <-subscriber.SubscribeHead():
		if up.Network != "hoodi" || up.Slot != 42 || up.Root != "0xdeadbeef" {
			t.Fatalf("unexpected head update: slot=%d root=%s", up.Slot, up.Root)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for head update")
	}
}

func TestRedisState_PublishFinalizedMonotonic(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rs := newTestRedisState(t, mr)

	rs.PublishFinalized(10)
	rs.PublishFinalized(8) // lower — should be ignored
	rs.PublishFinalized(12)

	if got := rs.GetFinalized(); got != 12 {
		t.Fatalf("finalized epoch: got %d want 12", got)
	}
}

func TestRedisState_FinalizedPersistsAcrossInstances(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	rs1 := newTestRedisState(t, mr)
	rs1.PublishFinalized(99)

	// A new instance should load the persisted epoch on startup.
	rs2 := newTestRedisState(t, mr)
	if got := rs2.GetFinalized(); got != 99 {
		t.Fatalf("new instance finalized epoch: got %d want 99", got)
	}
}

func TestRedisState_Password(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	mr.RequireAuth("s3cr3t")

	_, err := NewRedisState(&config.RedisStateConfig{URL: "redis://" + mr.Addr()})
	if err == nil {
		t.Fatal("expected error connecting without password")
	}

	rs, err := NewRedisState(&config.RedisStateConfig{
		URL:      "redis://" + mr.Addr(),
		Password: "s3cr3t",
	})
	if err != nil {
		t.Fatalf("expected success with correct password: %v", err)
	}
	t.Cleanup(func() { rs.Close() }) //nolint:errcheck
}
