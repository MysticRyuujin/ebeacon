package upstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/state"
	"github.com/mysticryuujin/ebeacon/upstream"
)

// newMinimalPool creates a Pool with one fake upstream for state tests.
// Health monitoring is not started, so the fake URL is never contacted.
func newMinimalPool(t *testing.T, id string) *upstream.Pool {
	t.Helper()
	pool, err := upstream.NewPool(
		id,
		[]config.UpstreamConfig{{ID: "fake", URL: "http://127.0.0.1:19999"}},
		config.RoutingConfig{LoadBalancing: "round-robin"},
		config.HealthConfig{
			CheckInterval:    time.Minute,
			FinalityInterval: time.Minute,
			MaxSyncDistance:  10,
			FollowDistance:   32,
			MaxHeadDistance:  2,
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func newRedisState(t *testing.T, mr *miniredis.Miniredis) *state.RedisState {
	t.Helper()
	rs, err := state.NewRedisState(&config.RedisStateConfig{
		URL: "redis://" + mr.Addr(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rs.Close() }) //nolint:errcheck
	return rs
}

// TestPool_SharedState_FinalizedEpochSeeding verifies that a pool seeds its
// finalized epoch from shared state on startup.
func TestPool_SharedState_FinalizedEpochSeeding(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	// Instance A discovers epoch 50 and publishes it. Both instances serve
	// the same network; the finalized epoch is shared per network.
	stateA := newRedisState(t, mr)
	poolA := newMinimalPool(t, "net-a1")
	poolA.SetSharedState(stateA)
	poolA.UpdateFinalizedEpoch(50)

	// Instance B starts up and seeds finalized epoch from Redis.
	stateB := newRedisState(t, mr)
	poolB := newMinimalPool(t, "net-a1")
	poolB.SetSharedState(stateB)
	poolB.StartStateSync()

	want := uint64(50 * 32)
	if got := poolB.FinalizedSlot(); got != want {
		t.Fatalf("finalized slot after seeding: got %d want %d", got, want)
	}
}

func TestPool_SharedState_FinalizedEpochDoesNotCrossNetworks(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	stateA := newRedisState(t, mr)
	poolMainnet := newMinimalPool(t, "mainnet-x")
	poolMainnet.SetSharedState(stateA)
	poolMainnet.UpdateFinalizedEpoch(300000)

	stateB := newRedisState(t, mr)
	poolSepolia := newMinimalPool(t, "sepolia-x")
	poolSepolia.SetSharedState(stateB)
	poolSepolia.StartStateSync()

	if got := poolSepolia.FinalizedSlot(); got != 0 {
		t.Fatalf("sepolia pool seeded from mainnet's finalized epoch: got slot %d", got)
	}
}

// TestPool_SharedState_FinalizedEpochPublish verifies that UpdateFinalizedEpoch
// publishes to shared state so other instances can read it.
func TestPool_SharedState_FinalizedEpochPublish(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	stateA := newRedisState(t, mr)
	poolA := newMinimalPool(t, "net-a2")
	poolA.SetSharedState(stateA)

	poolA.UpdateFinalizedEpoch(100)

	// Verify the epoch is visible in shared state (simulating what a new instance reads).
	if got := stateA.GetFinalized("net-a2"); got != 100 {
		t.Fatalf("shared state finalized epoch: got %d want 100", got)
	}
}

// TestPool_SharedState_HeadPublish verifies that SyncCanonicalHead publishes
// the canonical head when it changes.
func TestPool_SharedState_HeadPublish(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	stateA := newRedisState(t, mr)
	poolA := newMinimalPool(t, "net-a3")
	poolA.SetSharedState(stateA)

	// Allow the background subscription goroutine to establish before publishing.
	time.Sleep(50 * time.Millisecond)

	// Simulate a head block seen by an upstream.
	poolA.BlockCache().AddBlock("upstream-1", 200, "0xabc", "0xdef")
	poolA.SyncCanonicalHead()

	// The head update should be on the subscription channel.
	select {
	case update := <-stateA.SubscribeHead():
		if update.Network != "net-a3" || update.Slot != 200 || update.Root != "0xabc" {
			t.Fatalf("unexpected head update: slot=%d root=%s", update.Slot, update.Root)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for head update on shared state")
	}
}

// TestPool_SharedState_HeadDedup verifies that SyncCanonicalHead does not
// publish redundant updates when the canonical head has not changed.
func TestPool_SharedState_HeadDedup(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	stateA := newRedisState(t, mr)
	poolA := newMinimalPool(t, "net-a4")
	poolA.SetSharedState(stateA)

	// Allow the background subscription goroutine to establish before publishing.
	time.Sleep(50 * time.Millisecond)

	poolA.BlockCache().AddBlock("upstream-1", 300, "0xfeed", "0xbeef")
	poolA.SyncCanonicalHead() // first: publishes
	poolA.SyncCanonicalHead() // second: same head, should not publish

	// Drain the one expected message.
	select {
	case <-stateA.SubscribeHead():
	case <-time.After(time.Second):
		t.Fatal("expected at least one head update")
	}

	// Channel should now be empty.
	select {
	case extra := <-stateA.SubscribeHead():
		t.Fatalf("unexpected duplicate head update: %+v", extra)
	case <-time.After(100 * time.Millisecond):
		// good — no duplicate
	}
}

// TestPool_SharedState_RemoteHeadRecorded verifies that RecordRemoteHead
// contributes to the pool's canonical head determination.
func TestPool_SharedState_RemoteHeadRecorded(t *testing.T) {
	t.Parallel()

	pool := newMinimalPool(t, "net-remote")

	// No local upstream has reported a block yet.
	slot, root := pool.BlockCache().CanonicalHead()
	if slot != 0 || root != "" {
		t.Fatalf("expected empty canonical head, got slot=%d root=%s", slot, root)
	}

	// A remote instance publishes a head block.
	pool.RecordRemoteHead(500, "0xcafe")

	slot, root = pool.BlockCache().CanonicalHead()
	if slot != 500 || root != "0xcafe" {
		t.Fatalf("canonical head after remote record: slot=%d root=%s", slot, root)
	}
}

// TestPool_SharedState_CrossInstanceHeadPropagation verifies the full pub/sub
// loop: pool A publishes a canonical head; a fan-out goroutine delivers it to
// the matching pool via RecordRemoteHead.
func TestPool_SharedState_CrossInstanceHeadPropagation(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	stateA := newRedisState(t, mr)
	stateB := newRedisState(t, mr)

	poolA := newMinimalPool(t, "net-fanout")
	poolB := newMinimalPool(t, "net-fanout")
	poolA.SetSharedState(stateA)
	poolB.SetSharedState(stateB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pools := map[string]*upstream.Pool{
		"net-fanout": poolB,
	}

	// Fan-out goroutine (mirrors main.go).
	go func() {
		ch := stateB.SubscribeHead()
		for {
			select {
			case <-ctx.Done():
				return
			case update, ok := <-ch:
				if !ok {
					return
				}
				pool, ok := pools[update.Network]
				if !ok {
					continue
				}
				pool.RecordRemoteHead(update.Slot, update.Root)
			}
		}
	}()

	// Allow subscription to establish before publishing.
	time.Sleep(50 * time.Millisecond)

	// Pool A sees a new canonical head and publishes it.
	poolA.BlockCache().AddBlock("upstream-x", 42, "0x1234", "0x0000")
	poolA.SyncCanonicalHead()

	// Pool B should receive and record it.
	deadline := time.After(2 * time.Second)
	for {
		slot, root := poolB.BlockCache().CanonicalHead()
		if slot == 42 && root == "0x1234" {
			return // success
		}
		select {
		case <-deadline:
			t.Fatalf("pool B did not receive canonical head after 2s: slot=%d root=%s", slot, root)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestPool_SharedState_CrossNetworkIsolation verifies that a head published for
// one network is not recorded in a different network's pool.
func TestPool_SharedState_CrossNetworkIsolation(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	stateA := newRedisState(t, mr)
	stateB := newRedisState(t, mr)

	poolA := newMinimalPool(t, "hoodi")
	poolB := newMinimalPool(t, "sepolia")
	poolA.SetSharedState(stateA)
	poolB.SetSharedState(stateB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pools := map[string]*upstream.Pool{
		"hoodi":   poolA,
		"sepolia": poolB,
	}

	go func() {
		ch := stateB.SubscribeHead()
		for {
			select {
			case <-ctx.Done():
				return
			case update, ok := <-ch:
				if !ok {
					return
				}
				pool, ok := pools[update.Network]
				if !ok {
					continue
				}
				pool.RecordRemoteHead(update.Slot, update.Root)
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)

	poolA.BlockCache().AddBlock("upstream-x", 99, "0xaaaa", "0x0000")
	poolA.SyncCanonicalHead()

	deadline := time.After(500 * time.Millisecond)
	for {
		slot, root := poolB.BlockCache().CanonicalHead()
		if slot != 0 || root != "" {
			t.Fatalf("pool B should not receive hoodi head: slot=%d root=%s", slot, root)
		}
		select {
		case <-deadline:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
