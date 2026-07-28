package upstream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
)

func cbUpstream(failureThreshold, successThreshold int, halfOpenAfter time.Duration) *Upstream {
	return New("test", config.UpstreamConfig{
		ID:  "node-a",
		URL: "http://node-a",
		Failsafe: &config.FailsafeConfig{
			CircuitBreaker: &config.CircuitBreakerConfig{
				FailureThreshold: failureThreshold,
				SuccessThreshold: successThreshold,
				HalfOpenAfter:    halfOpenAfter,
			},
		},
	}, nil, 10)
}

func TestCircuitBreakerHalfOpenAllowsSingleProbe(t *testing.T) {
	u := cbUpstream(1, 2, time.Millisecond)

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
	tokens := make(chan CBToken, contenders)
	var wg sync.WaitGroup
	wg.Add(contenders)
	for range contenders {
		go func() {
			defer wg.Done()
			<-start
			if tok, ok := u.CBTryAcquire(); ok {
				acquired.Add(1)
				tokens <- tok
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

	(<-tokens).Success()
	if !u.IsReady() {
		t.Fatal("successful probe should make the next sequential recovery probe available")
	}
	tok, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("second sequential recovery probe was not acquired")
	}
	tok.Success()
	if !u.IsReady() {
		t.Fatal("success threshold should close the circuit")
	}
}

func TestCircuitBreakerReleaseAbandonsProbe(t *testing.T) {
	u := cbUpstream(1, 1, time.Millisecond)
	u.CBFailure()
	time.Sleep(2 * time.Millisecond)

	tok, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("failed to acquire recovery probe")
	}
	tok.Release()
	if _, ok := u.CBTryAcquire(); !ok {
		t.Fatal("released recovery probe was not made available")
	}
}

func TestCircuitBreakerStaleProbeTokenCannotDisturbNewProbe(t *testing.T) {
	u := cbUpstream(1, 2, time.Millisecond)
	u.CBFailure()
	time.Sleep(2 * time.Millisecond)

	first, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("failed to acquire first recovery probe")
	}
	first.Success()

	second, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("failed to acquire second recovery probe")
	}

	// The first request has already settled; a late release or failure from it
	// must not free or fail the probe the second request now owns.
	first.Release()
	first.Failure()
	if _, ok := u.CBTryAcquire(); ok {
		t.Fatal("stale settlement freed the in-flight recovery probe")
	}
	if u.IsReady() {
		t.Fatal("stale settlement made a busy half-open circuit routable")
	}

	second.Success()
	if !u.IsReady() {
		t.Fatal("success threshold should close the circuit")
	}
}

func TestCircuitBreakerUntokenizedFailureReopensRecoveringCircuit(t *testing.T) {
	u := cbUpstream(1, 2, time.Millisecond)
	u.CBFailure()
	time.Sleep(2 * time.Millisecond)

	probe, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("failed to acquire recovery probe")
	}
	probe.Success()
	if !u.IsReady() {
		t.Fatal("circuit should still offer probes below the success threshold")
	}

	// A stream that already credited its probe reports a later failure. Losing
	// it would let the next single success close the circuit as if the upstream
	// had recovered cleanly.
	u.CBFailure()
	if u.IsReady() {
		t.Fatal("a failure during recovery must reopen the circuit")
	}

	time.Sleep(2 * time.Millisecond)
	next, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("reopened circuit should offer a fresh probe after the interval")
	}
	next.Success()

	// Still half-open, not closed: the reopen reset the success count, so this
	// lone success cannot satisfy a threshold of two. A closed circuit would
	// admit both acquisitions; a half-open one grants a single probe.
	if _, ok := u.CBTryAcquire(); !ok {
		t.Fatal("recovering circuit should grant the next sequential probe")
	}
	if _, ok := u.CBTryAcquire(); ok {
		t.Fatal("one success after a recovery failure must not close the circuit")
	}
}

func TestCircuitBreakerProbeSettleIsIdempotent(t *testing.T) {
	u := cbUpstream(1, 1, time.Millisecond)
	u.CBFailure()
	time.Sleep(2 * time.Millisecond)

	tok, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("failed to acquire recovery probe")
	}
	tok.Success()
	tok.Failure()
	if !u.IsReady() {
		t.Fatal("a duplicate probe settlement must not reopen the circuit")
	}
}

func TestCircuitBreakerStaleTokenCannotDisturbRecoveredCircuit(t *testing.T) {
	u := cbUpstream(2, 1, time.Millisecond)

	stale, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("closed circuit must admit")
	}

	u.CBFailure()
	u.CBFailure()
	time.Sleep(2 * time.Millisecond)
	probe, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("failed to acquire recovery probe")
	}
	probe.Success()
	if !u.IsReady() {
		t.Fatal("circuit should have closed after a successful probe")
	}

	// The stale request was admitted before the outage; neither outcome may
	// touch the counters of the circuit that has since recovered.
	stale.Failure()
	if !u.IsReady() {
		t.Fatal("stale failure reopened the recovered circuit")
	}
	u.CBFailure()
	u.CBFailure()
	if u.IsReady() {
		t.Fatal("post-recovery failures should reopen the circuit")
	}
}

func TestCircuitBreakerOpenCircuitAdmitsAdvisoryDispatch(t *testing.T) {
	u := cbUpstream(1, 1, 50*time.Millisecond)
	u.CBFailure()

	if u.IsReady() {
		t.Fatal("open circuit must not be preferred for routing")
	}
	// Last-resort candidates still dispatch: the tier fallback in the pool
	// hands out open upstreams when nothing better exists, and refusing here
	// would turn the worst available option into a hard 502.
	first, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("open circuit must still admit a last-resort dispatch")
	}
	if _, ok := u.CBTryAcquire(); !ok {
		t.Fatal("advisory admission must not consume a recovery probe")
	}
	first.Failure()

	time.Sleep(60 * time.Millisecond)
	probe, ok := u.CBTryAcquire()
	if !ok {
		t.Fatal("advisory failures must not extend the recovery window")
	}
	if _, ok := u.CBTryAcquire(); ok {
		t.Fatal("half-open circuit granted a second concurrent probe")
	}
	probe.Success()
	if !u.IsReady() {
		t.Fatal("successful probe should close the circuit")
	}
}
