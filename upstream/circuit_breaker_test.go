package upstream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
)

func TestCircuitBreakerHalfOpenAllowsSingleProbe(t *testing.T) {
	u := New("test", config.UpstreamConfig{
		ID:  "node-a",
		URL: "http://node-a",
		Failsafe: &config.FailsafeConfig{
			CircuitBreaker: &config.CircuitBreakerConfig{
				FailureThreshold: 1,
				SuccessThreshold: 2,
				HalfOpenAfter:    time.Millisecond,
			},
		},
	}, nil, 10)

	u.CBFailure()
	time.Sleep(2 * time.Millisecond)

	// Readiness checks must not consume the recovery probe.
	if !u.IsReady() {
		t.Fatal("elapsed open circuit should report ready for a recovery probe")
	}
	if !u.IsReady() {
		t.Fatal("readiness inspection consumed the recovery probe")
	}

	const contenders = 32
	start := make(chan struct{})
	var acquired atomic.Int32
	var wg sync.WaitGroup
	wg.Add(contenders)
	for range contenders {
		go func() {
			defer wg.Done()
			<-start
			if u.CBTryAcquire() {
				acquired.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := acquired.Load(); got != 1 {
		t.Fatalf("half-open acquisitions: got %d want 1", got)
	}
	if u.IsReady() {
		t.Fatal("half-open circuit should not route another request while the probe is in flight")
	}

	u.CBSuccess()
	if !u.IsReady() {
		t.Fatal("successful probe should make the next sequential recovery probe available")
	}
	if !u.CBTryAcquire() {
		t.Fatal("second sequential recovery probe was not acquired")
	}
	u.CBSuccess()
	if !u.IsReady() {
		t.Fatal("success threshold should close the circuit")
	}
}

func TestCircuitBreakerReleaseAbandonsProbe(t *testing.T) {
	u := New("test", config.UpstreamConfig{
		ID:  "node-a",
		URL: "http://node-a",
		Failsafe: &config.FailsafeConfig{
			CircuitBreaker: &config.CircuitBreakerConfig{
				FailureThreshold: 1,
				SuccessThreshold: 1,
				HalfOpenAfter:    time.Millisecond,
			},
		},
	}, nil, 10)
	u.CBFailure()
	time.Sleep(2 * time.Millisecond)

	if !u.CBTryAcquire() {
		t.Fatal("failed to acquire recovery probe")
	}
	u.CBRelease()
	if !u.CBTryAcquire() {
		t.Fatal("released recovery probe was not made available")
	}
}
