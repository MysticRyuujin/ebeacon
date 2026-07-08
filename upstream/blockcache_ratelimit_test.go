package upstream

import (
	"testing"
	"time"
)

func TestBlockCache_IsOnCanonicalFork_DetectsCompetingHead(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	// Canonical head observed by 2/3 upstreams at slot 100.
	bc.AddBlock("a", 100, "root-canon", "p99")
	bc.AddBlock("b", 100, "root-canon", "p99")
	// Competing head at same slot from upstream c.
	bc.AddBlock("c", 100, "root-fork", "p99")

	if !bc.IsOnCanonicalFork("a") || !bc.IsOnCanonicalFork("b") {
		t.Fatal("canonical reporters should be considered on canonical fork")
	}
	if bc.IsOnCanonicalFork("c") {
		t.Fatal("competing head at canonical slot should be treated as forked")
	}
}

func TestBlockCache_IsOnCanonicalFork_AllowsSmallLag(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	bc.AddBlock("a", 100, "root-canon", "p99")
	bc.AddBlock("b", 100, "root-canon", "p99")
	bc.AddBlock("c", 99, "root-lag", "p98")

	if !bc.IsOnCanonicalFork("c") {
		t.Fatal("upstream one slot behind should still be treated as canonical")
	}
}

func TestBlockCache_ForkStatus_Canonical(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	bc.AddBlock("a", 100, "root-canon", "p99")
	bc.AddBlock("b", 100, "root-canon", "p99")

	if s := bc.ForkStatus("a"); s != "canonical" {
		t.Fatalf("expected canonical, got %s", s)
	}
}

func TestBlockCache_ForkStatus_Forked(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	bc.AddBlock("a", 100, "root-canon", "p99")
	bc.AddBlock("b", 100, "root-canon", "p99")
	bc.AddBlock("c", 100, "root-fork", "p99")

	if s := bc.ForkStatus("c"); s != "forked" {
		t.Fatalf("competing head at canonical slot should be forked, got %s", s)
	}
}

func TestBlockCache_ForkStatus_LaggingWithinDistance(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	bc.AddBlock("a", 100, "root-canon", "p99")
	bc.AddBlock("b", 100, "root-canon", "p99")
	bc.AddBlock("c", 99, "root-lag", "p98")

	if s := bc.ForkStatus("c"); s != "canonical" {
		t.Fatalf("upstream one slot behind should be canonical, got %s", s)
	}
}

func TestBlockCache_ForkStatus_LaggingBeyondDistance(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	bc.AddBlock("a", 100, "root-canon", "p99")
	bc.AddBlock("b", 100, "root-canon", "p99")
	bc.AddBlock("c", 95, "root-old", "p94")

	if s := bc.ForkStatus("c"); s != "lagging" {
		t.Fatalf("upstream far behind should be lagging, got %s", s)
	}
	// IsOnCanonicalFork still returns false for routing purposes.
	if bc.IsOnCanonicalFork("c") {
		t.Fatal("upstream far behind should not be on canonical fork for routing")
	}
}

func TestBlockCache_CanonicalHeadSeenBy_ReturnsReporters(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	bc.AddBlock("a", 100, "root-canon", "p99")
	bc.AddBlock("b", 100, "root-canon", "p99")
	// Competing head at the same slot; should not appear in the canonical set.
	bc.AddBlock("c", 100, "root-fork", "p99")
	// Lagging upstream one slot back; should not appear either.
	bc.AddBlock("d", 99, "root-prev", "p98")

	seen := bc.CanonicalHeadSeenBy()
	if len(seen) != 2 || !seen["a"] || !seen["b"] {
		t.Fatalf("expected {a,b} as canonical head reporters, got %v", seen)
	}
	if seen["c"] || seen["d"] {
		t.Fatalf("forked/lagging upstreams must not appear in canonical set: %v", seen)
	}
}

func TestBlockCache_CanonicalHeadSeenBy_EmptyBeforeAnyBlocks(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	if got := bc.CanonicalHeadSeenBy(); got != nil {
		t.Fatalf("expected nil before any blocks observed, got %v", got)
	}
}

func TestBlockCache_CanonicalHeadSeenBy_ExcludesRemote(t *testing.T) {
	t.Parallel()
	bc := NewBlockCache(32, 2)

	// A canonical head reported only by another ebeacon instance via shared
	// state cannot serve client requests locally and must not appear.
	bc.AddBlock("remote", 100, "root-canon", "p99")

	if got := bc.CanonicalHeadSeenBy(); got != nil {
		t.Fatalf("expected nil when only remote has reported, got %v", got)
	}

	// Once a local upstream reports the same block, it becomes eligible.
	bc.AddBlock("a", 100, "root-canon", "p99")
	seen := bc.CanonicalHeadSeenBy()
	if len(seen) != 1 || !seen["a"] {
		t.Fatalf("expected {a} after local reports head, got %v", seen)
	}
	if seen["remote"] {
		t.Fatalf("remote pseudo-ID must never appear in canonical set: %v", seen)
	}
}

func TestAutoTuner_AdjustsRateOn429(t *testing.T) {
	t.Parallel()
	at := NewAutoTuner(10, 1, 100, time.Second, 0.2)

	initial := at.currentRate
	at.lastAdjust = time.Now().Add(-2 * time.Second)
	at.RecordResponse(429)

	if at.currentRate >= initial {
		t.Fatalf("expected rate decrease after throttling, got %f from %f", at.currentRate, initial)
	}
}

func TestAutoTuner_AdjustsRateUpWhenHealthy(t *testing.T) {
	t.Parallel()
	at := NewAutoTuner(10, 1, 100, time.Second, 0.2)

	initial := at.currentRate
	at.lastAdjust = time.Now().Add(-2 * time.Second)
	at.RecordResponse(200)

	if at.currentRate <= initial {
		t.Fatalf("expected rate increase for healthy responses, got %f from %f", at.currentRate, initial)
	}
}

func TestAutoTuner_CandidacyCheckDoesNotConsume(t *testing.T) {
	t.Parallel()
	at := NewAutoTuner(2, 1, 100, time.Minute, 0.1)
	for range 50 {
		if !at.HasCapacity() {
			t.Fatal("repeated capacity checks must not drain the bucket")
		}
	}
	at.Consume()
	at.Consume()
	if at.HasCapacity() {
		t.Fatal("two consumes at burst 2 should drain the bucket")
	}
}

func TestAutoTuner_FractionalInitialRateAllowsFirstRequest(t *testing.T) {
	t.Parallel()
	at := NewAutoTuner(0.5, 0.1, 100, time.Minute, 0.1)
	if !at.HasCapacity() {
		t.Fatal("fractional initial rate must yield burst >= 1")
	}
}
