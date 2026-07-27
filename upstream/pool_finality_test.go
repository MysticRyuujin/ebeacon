package upstream_test

import (
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/upstream"
)

func newMultiPool(t *testing.T, networkID string, ids ...string) *upstream.Pool {
	t.Helper()
	cfgs := make([]config.UpstreamConfig, 0, len(ids))
	for _, id := range ids {
		cfgs = append(cfgs, config.UpstreamConfig{ID: id, URL: "http://127.0.0.1:19999"})
	}
	pool, err := upstream.NewPool(
		networkID,
		cfgs,
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

func TestPool_UpdateFinalizedEpoch_RejectsImplausibleEpoch(t *testing.T) {
	t.Parallel()
	pool := newMinimalPool(t, "net-fin-guard")
	pool.BlockCache().SetSlotTiming(time.Now().Unix()-100*32*12, 12)
	pool.SetSlotsPerEpoch(32)

	if got := pool.UpdateFinalizedEpoch(90); got != 90 {
		t.Fatalf("plausible epoch rejected: got %d want 90", got)
	}
	if got := pool.UpdateFinalizedEpoch(5_000_000); got != 90 {
		t.Fatalf("implausible epoch accepted: got %d want 90", got)
	}
	if got := pool.UpdateFinalizedEpoch(math.MaxUint64); got != 90 {
		t.Fatalf("MaxUint64 epoch accepted (overflow in plausibility check): got %d want 90", got)
	}
	if got := pool.FinalizedEpoch(); got != 90 {
		t.Fatalf("pool epoch poisoned: got %d want 90", got)
	}
}

func TestPool_UpdateFinalizedEpoch_UnknownGenesisFailsOpen(t *testing.T) {
	t.Parallel()
	pool := newMinimalPool(t, "net-fin-open")

	if got := pool.UpdateFinalizedEpoch(5_000_000); got != 5_000_000 {
		t.Fatalf("unknown genesis must fail open: got %d", got)
	}
}

func TestPool_StartStateSync_RejectsImplausibleSeed(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	st := newRedisState(t, mr)
	st.PublishFinalized("net-fin-seed", 5_000_000)

	pool := newMinimalPool(t, "net-fin-seed")
	pool.BlockCache().SetSlotTiming(time.Now().Unix()-100*32*12, 12)
	pool.SetSlotsPerEpoch(32)
	pool.SetSharedState(st)
	pool.StartStateSync()

	if got := pool.FinalizedEpoch(); got != 0 {
		t.Fatalf("poisoned shared state reinstated on seed: got %d want 0", got)
	}
}

func TestPool_UpdateFinalizedEpoch_RetriesPublishAfterFailure(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	st := newRedisState(t, mr)
	pool := newMinimalPool(t, "net-fin-retry")
	pool.SetSharedState(st)

	mr.SetError("redis unavailable")
	pool.UpdateFinalizedEpoch(50)
	mr.SetError("")
	if got := st.GetFinalized("net-fin-retry"); got != 0 {
		t.Fatalf("write should have failed, got %d", got)
	}

	// The same epoch must still be offered to shared state so the failed
	// write is retried on the next finality probe.
	pool.UpdateFinalizedEpoch(50)
	if got := st.GetFinalized("net-fin-retry"); got != 50 {
		t.Fatalf("failed publish never retried: got %d want 50", got)
	}
}

func TestPool_ByGlob_PrefersReadyMatch(t *testing.T) {
	t.Parallel()
	pool := newMultiPool(t, "net-glob", "lh-1", "lh-2", "prysm")

	pool.ByID("lh-1").SetHealth(upstream.HealthDown)
	u := pool.ByGlob("lh-*")
	if u == nil || u.ID != "lh-2" {
		t.Fatalf("expected ready match lh-2, got %v", u)
	}

	pool.ByID("lh-2").SetHealth(upstream.HealthDown)
	if u := pool.ByGlob("lh-*"); u != nil {
		t.Fatalf("expected nil when no glob match is ready, got %s", u.ID)
	}
}
