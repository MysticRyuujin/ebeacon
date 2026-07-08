package upstream

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// AutoTuner dynamically adjusts an upstream's send rate based on 429 response frequency.
type AutoTuner struct {
	mu      sync.Mutex
	limiter *rate.Limiter

	totalCount    int
	throttleCount int
	lastAdjust    time.Time

	adjustmentPeriod   time.Duration
	errorRateThreshold float64
	increaseFactor     float64
	decreaseFactor     float64
	minRate            float64
	maxRate            float64
	currentRate        float64
}

// NewAutoTuner creates an AutoTuner with the given parameters.
func NewAutoTuner(initialRate, minRate, maxRate float64, adjustmentPeriod time.Duration, errorRateThreshold float64) *AutoTuner {
	if initialRate <= 0 {
		initialRate = 100
	}
	if minRate <= 0 {
		minRate = 1
	}
	if maxRate <= 0 {
		maxRate = 10000
	}
	if adjustmentPeriod <= 0 {
		adjustmentPeriod = time.Minute
	}
	if errorRateThreshold <= 0 {
		errorRateThreshold = 0.1
	}

	return &AutoTuner{
		limiter:            rate.NewLimiter(rate.Limit(initialRate), max(int(initialRate), 1)),
		lastAdjust:         time.Now(),
		adjustmentPeriod:   adjustmentPeriod,
		errorRateThreshold: errorRateThreshold,
		increaseFactor:     1.05,
		decreaseFactor:     0.9,
		minRate:            minRate,
		maxRate:            maxRate,
		currentRate:        initialRate,
	}
}

// Allow returns true if the request should be allowed under the current rate.
func (at *AutoTuner) Allow() bool {
	return at.limiter.Allow()
}

// RecordResponse records a response status code. 429 responses trigger rate adjustment.
func (at *AutoTuner) RecordResponse(statusCode int) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.totalCount++
	if statusCode == 429 {
		at.throttleCount++
	}

	if time.Since(at.lastAdjust) >= at.adjustmentPeriod {
		at.adjust()
	}
}

func (at *AutoTuner) adjust() {
	if at.totalCount == 0 {
		at.lastAdjust = time.Now()
		return
	}

	errRate := float64(at.throttleCount) / float64(at.totalCount)

	if errRate > at.errorRateThreshold {
		at.currentRate *= at.decreaseFactor
		if at.currentRate < at.minRate {
			at.currentRate = at.minRate
		}
	} else {
		at.currentRate *= at.increaseFactor
		if at.currentRate > at.maxRate {
			at.currentRate = at.maxRate
		}
	}

	at.limiter.SetLimit(rate.Limit(at.currentRate))
	at.limiter.SetBurst(max(int(at.currentRate), 1))
	at.totalCount = 0
	at.throttleCount = 0
	at.lastAdjust = time.Now()
}
