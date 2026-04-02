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
	mu         sync.RWMutex
	windowSize int
	latencies  []time.Duration
	errors     []bool // true = error
	pos        int
	count      int
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
func (s *ScoreTracker) RecordSuccess(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.pos % s.windowSize
	s.latencies[idx] = d
	s.errors[idx] = false
	s.pos++
	if s.count < s.windowSize {
		s.count++
	}
}

// RecordError records a failed request.
func (s *ScoreTracker) RecordError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.pos % s.windowSize
	s.latencies[idx] = 0
	s.errors[idx] = true
	s.pos++
	if s.count < s.windowSize {
		s.count++
	}
}

// ErrorRate returns the proportion of errors in the rolling window [0.0, 1.0].
func (s *ScoreTracker) ErrorRate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.count == 0 {
		return 0
	}
	errs := 0
	for i := 0; i < s.count; i++ {
		if s.errors[i] {
			errs++
		}
	}
	return float64(errs) / float64(s.count)
}

// P90Latency returns the 90th percentile latency of successful requests.
// P90 is preferred over mean because beacon chain operations are deadline-driven:
// a validator must attest within 4 seconds of the slot start or lose rewards.
// Mean latency can look acceptable while P90 spikes indicate tail-latency
// problems that would cause intermittent missed attestations.
func (s *ScoreTracker) P90Latency() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	successful := make([]time.Duration, 0, s.count)
	for i := 0; i < s.count; i++ {
		if !s.errors[i] && s.latencies[i] > 0 {
			successful = append(successful, s.latencies[i])
		}
	}
	if len(successful) == 0 {
		return 0
	}

	sort.Slice(successful, func(i, j int) bool { return successful[i] < successful[j] })

	idx := int(math.Ceil(float64(len(successful))*0.9)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(successful) {
		idx = len(successful) - 1
	}
	return successful[idx]
}

// Snapshot returns the current rolling score inputs under a single lock.
func (s *ScoreTracker) Snapshot() ScoreSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

// Score computes a composite quality score (higher = better).
// headLag is the number of slots behind the pool's canonical head.
// syncDist is the upstream's reported sync distance.
func (s *ScoreTracker) Score(errorWeight, latencyWeight, headLagWeight, syncDistWeight float64, headLag, syncDist uint64) float64 {
	snapshot := s.Snapshot()
	return calculateScore(
		errorWeight,
		latencyWeight,
		headLagWeight,
		syncDistWeight,
		snapshot.ErrorRate,
		snapshot.P90Latency,
		headLag,
		syncDist,
	)
}
