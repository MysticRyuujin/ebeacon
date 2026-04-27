package upstream

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
)

var routeScoreLabelSeq atomic.Uint64

func routeScorePoolID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("route_score_%d_%s", routeScoreLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_"))
}

func TestPool_SelectForPath_UsesRouteScopedScores(t *testing.T) {
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(routeScorePoolID(t), []config.UpstreamConfig{
		{ID: "global-fast", URL: "http://a", Weight: 1},
		{ID: "validator-fast", URL: "http://b", Weight: 1},
	}, config.RoutingConfig{LoadBalancing: "score", ScoreWindowSize: 20}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	globalFast := p.ByID("global-fast")
	validatorFast := p.ByID("validator-fast")
	globalFast.UpdateSyncStatus(false, 128, 0)
	validatorFast.UpdateSyncStatus(false, 128, 0)
	p.BlockCache().AddBlock("global-fast", 128, "0x1", "0x0")
	p.BlockCache().AddBlock("validator-fast", 128, "0x1", "0x0")

	for range 10 {
		globalFast.RecordSuccessForPath("/eth/v1/node/version", 5*time.Millisecond)
		validatorFast.RecordSuccessForPath("/eth/v1/node/version", 2*time.Second)

		globalFast.RecordSuccessForPath("/eth/v1/beacon/states/{state_id}/validators", 900*time.Millisecond)
		validatorFast.RecordSuccessForPath("/eth/v1/beacon/states/{state_id}/validators", 15*time.Millisecond)
	}

	globalValidator := globalFast.ScoreSnapshotForPath("/eth/v1/beacon/states/{state_id}/validators")
	fastValidator := validatorFast.ScoreSnapshotForPath("/eth/v1/beacon/states/{state_id}/validators")
	if globalValidator.Samples != 10 || fastValidator.Samples != 10 {
		t.Fatalf("unexpected route-scoped samples: global=%d validator=%d", globalValidator.Samples, fastValidator.Samples)
	}
	if globalValidator.P90Latency <= fastValidator.P90Latency {
		t.Fatalf("expected global-fast validator snapshot to be slower: global=%v validator=%v", globalValidator.P90Latency, fastValidator.P90Latency)
	}
	globalDetails := p.scoreDetailsWithWeights(globalFast, p.BlockCache().MaxSlot(), p.effectiveScoreWeights(), "/eth/v1/beacon/states/{state_id}/validators")
	fastDetails := p.scoreDetailsWithWeights(validatorFast, p.BlockCache().MaxSlot(), p.effectiveScoreWeights(), "/eth/v1/beacon/states/{state_id}/validators")
	if globalDetails.Score >= fastDetails.Score {
		t.Fatalf("expected validator-fast route score to be higher: global=%f validator=%f", globalDetails.Score, fastDetails.Score)
	}

	globalCounts := map[string]int{}
	validatorCounts := map[string]int{}
	for range 300 {
		globalCounts[p.SelectForPath("/eth/v1/node/version", 1)[0].ID]++
		validatorCounts[p.SelectForPath("/eth/v1/beacon/states/{state_id}/validators", 1)[0].ID]++
	}

	if globalCounts["global-fast"] <= globalCounts["validator-fast"] {
		t.Fatalf("expected global-fast to win node/version selection more often: %+v", globalCounts)
	}
	if validatorCounts["validator-fast"] <= validatorCounts["global-fast"] {
		t.Fatalf("expected validator-fast to win validators selection more often: %+v", validatorCounts)
	}
}

func TestUpstream_ScoreSnapshotForPath_FallsBackUntilEnoughSamples(t *testing.T) {
	u := New("net", config.UpstreamConfig{ID: "u1", URL: "http://a"}, nil, 10)
	for range 6 {
		u.RecordSuccess(10 * time.Millisecond)
	}
	for range 4 {
		u.RecordSuccessForPath("/eth/v1/beacon/states/{state_id}/validators", 500*time.Millisecond)
	}

	fallback := u.ScoreSnapshotForPath("/eth/v1/beacon/states/{state_id}/validators")
	if fallback.Samples != 10 {
		t.Fatalf("expected fallback to global snapshot before threshold, got %d samples", fallback.Samples)
	}

	u.RecordSuccessForPath("/eth/v1/beacon/states/{state_id}/validators", 500*time.Millisecond)
	routeScoped := u.ScoreSnapshotForPath("/eth/v1/beacon/states/{state_id}/validators")
	if routeScoped.Samples != 5 {
		t.Fatalf("expected route-scoped snapshot after threshold, got %d samples", routeScoped.Samples)
	}
	if routeScoped.P90Latency < 500*time.Millisecond {
		t.Fatalf("expected route-scoped p90 to reflect slow validator path, got %v", routeScoped.P90Latency)
	}
}
