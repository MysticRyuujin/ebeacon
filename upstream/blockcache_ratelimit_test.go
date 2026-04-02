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
