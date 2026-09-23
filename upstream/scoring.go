package upstream

import (
	"math"
	"sort"
	"sync"
	"time"
)

// ScoreSnapshot captures the rolling metrics used to compute an upstream score.
type ScoreSnapshot struct {
	Samples    int
	ErrorRate  float64
	P90Latency time.Duration
}

// ScoreTracker tracks rolling request metrics for computing an upstream's quality score.
type ScoreTracker struct {
	mu         sync.Mutex
	windowSize int
	latencies  []time.Duration
	errors     []bool // true = error
	pos        int
	count      int

	// snapshot caches Snapshot's result until the next record. Routing and
	// metrics read it several times per request; writes happen once.
	snapshot      ScoreSnapshot
	snapshotValid bool
}

// NewScoreTracker creates a ScoreTracker with the given rolling window size.
func NewScoreTracker(windowSize int) *ScoreTracker {
	if windowSize <= 0 {
		windowSize = 100
	}
	return &ScoreTracker{
		windowSize: windowSize,
		latencies:  make([]time.Duration, windowSize),
		errors:     make([]bool, windowSize),
	}
}

// RecordSuccess records a successful request with its latency.
func (s *ScoreTracker) RecordSuccess(d time.Duration) { s.record(d, false) }

// RecordError records a failed request.
func (s *ScoreTracker) RecordError() { s.record(0, true) }

func (s *ScoreTracker) record(d time.Duration, isErr bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.pos % s.windowSize
	s.latencies[idx] = d
	s.errors[idx] = isErr
	s.snapshotValid = false
	s.pos++
	if s.count < s.windowSize {
		s.count++
	}
}

// Snapshot returns the current rolling score inputs under a single lock.
func (s *ScoreTracker) Snapshot() ScoreSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.snapshotValid {
		s.snapshot = s.computeSnapshot()
		s.snapshotValid = true
	}
	return s.snapshot
}

func (s *ScoreTracker) computeSnapshot() ScoreSnapshot {
	snapshot := ScoreSnapshot{Samples: s.count}
	if s.count == 0 {
		return snapshot
	}

	errCount := 0
	successful := make([]time.Duration, 0, s.count)
	for i := 0; i < s.count; i++ {
		if s.errors[i] {
			errCount++
			continue
		}
		if s.latencies[i] > 0 {
			successful = append(successful, s.latencies[i])
		}
	}
	snapshot.ErrorRate = float64(errCount) / float64(s.count)
	if len(successful) == 0 {
		return snapshot
	}

	sort.Slice(successful, func(i, j int) bool { return successful[i] < successful[j] })
	idx := int(math.Ceil(float64(len(successful))*0.9)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(successful) {
		idx = len(successful) - 1
	}
	snapshot.P90Latency = successful[idx]
	return snapshot
}

// calculateScore computes a composite quality score (higher = better).
//
// Each component uses a different normalisation strategy:
//   - Error rate uses (1 - rate) so a 0% error rate contributes full weight.
//   - Latency, head lag, and sync distance use 1/(1+x) which maps [0, ∞) to
//     (0, 1]: a value of 0 scores 1.0, and the score degrades smoothly as the
//     value grows, never reaching zero. This avoids hard cliffs.
//
// Default weights (errorRate=4, latency=8, headLag=2, syncDist=1) reflect the
// relative impact on validators: high latency and errors are most damaging to
// attestation timing; head lag matters but is partially compensated by the
// canonical fork filter; sync distance is a secondary signal.
func calculateScore(errorWeight, latencyWeight, headLagWeight, syncDistWeight, errRate float64, p90Latency time.Duration, headLag, syncDist uint64) float64 {
	errorScore := (1 - errRate) * errorWeight
	latencyScore := latencyWeight
	if p90 := p90Latency.Seconds(); p90 > 0 {
		latencyScore = (1.0 / (1.0 + p90)) * latencyWeight
	}
	headLagScore := (1.0 / (1.0 + float64(headLag))) * headLagWeight
	syncDistScore := (1.0 / (1.0 + float64(syncDist))) * syncDistWeight
	return errorScore + latencyScore + headLagScore + syncDistScore
}
