package network

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/upstream"
)

func mkConsensusUpstream(id, rawURL string) *upstream.Upstream {
	return upstream.New("testnet", config.UpstreamConfig{ID: id, URL: rawURL}, &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    time.Second,
	}, 100)
}

func TestConsensusPolicy_ExecuteMajority(t *testing.T) {
	t.Parallel()
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"same"}`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"same"}`))
	}))
	defer b.Close()
	c := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"different"}`))
	}))
	defer c.Close()

	cp := &ConsensusPolicy{MaxParticipants: 3, AgreementThreshold: 2}
	ups := []*upstream.Upstream{
		mkConsensusUpstream("a", a.URL),
		mkConsensusUpstream("b", b.URL),
		mkConsensusUpstream("c", c.URL),
	}

	resp, picked, err := cp.Execute(context.Background(), ups, func(u *upstream.Upstream) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, u.URL, nil)
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if picked == nil {
		t.Fatal("expected a picked upstream")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	if !strings.Contains(string(body), "same") {
		t.Fatalf("expected majority response body, got %q", string(body))
	}
}

func TestConsensusPolicy_ExecuteNoConsensus(t *testing.T) {
	t.Parallel()
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`a`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`b`))
	}))
	defer b.Close()
	c := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`c`))
	}))
	defer c.Close()

	cp := &ConsensusPolicy{MaxParticipants: 3, AgreementThreshold: 2}
	ups := []*upstream.Upstream{
		mkConsensusUpstream("a", a.URL),
		mkConsensusUpstream("b", b.URL),
		mkConsensusUpstream("c", c.URL),
	}

	_, _, err := cp.Execute(context.Background(), ups, func(u *upstream.Upstream) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, u.URL, nil)
	})
	if err == nil {
		t.Fatal("expected consensus error")
	}
}

func TestNetwork_ConsensusWithGzipUpstreams(t *testing.T) {
	payload := `{"data":{"root":"0xabc"}}`
	gzHandler := func(level int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				w.Header().Set("Content-Encoding", "gzip")
				gz, _ := gzip.NewWriterLevel(w, level)
				_, _ = gz.Write([]byte(payload))
				_ = gz.Close()
				return
			}
			_, _ = w.Write([]byte(payload))
		}
	}
	// Different compression levels emulate per-node nondeterministic gzip
	// output for identical JSON.
	a := httptest.NewServer(gzHandler(gzip.BestSpeed))
	defer a.Close()
	b := httptest.NewServer(gzHandler(gzip.BestCompression))
	defer b.Close()

	id := netID(t)
	cfg := mustCfg(t, id, a.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "u1", URL: a.URL},
		{ID: "u2", URL: b.URL},
	}
	cfg.Failsafe.Consensus = &config.ConsensusConfig{
		Enabled:            true,
		MaxParticipants:    2,
		AgreementThreshold: 2,
	}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blocks/head/root", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q (gzip variance must not break consensus)", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Encoding") == "gzip" {
		t.Fatalf("small consensus body should not be gzip re-encoded")
	}
	if got := rec.Body.String(); got != payload {
		t.Fatalf("body: got %q want %q", got, payload)
	}
}

func TestConsensusPolicy_ConsumesRateTokens(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"same"}`))
	}))
	defer srv.Close()

	mkLimited := func(id string) *upstream.Upstream {
		return upstream.New("testnet", config.UpstreamConfig{
			ID:  id,
			URL: srv.URL,
			RateLimiting: &config.UpstreamRateLimitConfig{
				AutoTune:    true,
				InitialRate: 1, // burst 1: one request drains the bucket
				MinRate:     1,
				MaxRate:     10,
			},
		}, nil, 100)
	}

	ups := []*upstream.Upstream{mkLimited("a"), mkLimited("b")}
	if !ups[0].AllowRequest() {
		t.Fatal("precondition: fresh limiter should have capacity")
	}

	cp := &ConsensusPolicy{MaxParticipants: 2, AgreementThreshold: 2}
	_, _, err := cp.Execute(context.Background(), ups, func(u *upstream.Upstream) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, u.URL, nil)
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	for _, u := range ups {
		if u.AllowRequest() {
			t.Fatalf("upstream %s should have no capacity after consensus consumed its token", u.ID)
		}
	}
}

func TestConsensusPolicy_ClientCancelDoesNotTripBreaker(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // block until the request context is canceled
	}))
	defer srv.Close()

	// failureThreshold 1: a single CBFailure would open the breaker.
	mk := func(id string) *upstream.Upstream {
		return upstream.New("testnet", config.UpstreamConfig{ID: id, URL: srv.URL},
			&config.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, HalfOpenAfter: time.Hour}, 100)
	}
	ups := []*upstream.Upstream{mk("a"), mk("b")}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	cp := &ConsensusPolicy{MaxParticipants: 2, AgreementThreshold: 2}
	_, _, _ = cp.Execute(ctx, ups, func(u *upstream.Upstream) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, u.URL, nil)
	})

	for _, u := range ups {
		if !u.IsReady() {
			t.Fatalf("client cancel must not open %s's circuit breaker", u.ID)
		}
	}
}

func TestConsensusPolicy_LargestGroupWins(t *testing.T) {
	t.Parallel()
	majority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":"majority"}`))
	}))
	defer majority.Close()
	minority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":"minority"}`))
	}))
	defer minority.Close()

	// Threshold 1 qualifies both groups; the larger group must win every
	// time, not whichever a map walk happens to visit first.
	cp := &ConsensusPolicy{MaxParticipants: 3, AgreementThreshold: 1}
	ups := []*upstream.Upstream{
		mkConsensusUpstream("a", majority.URL),
		mkConsensusUpstream("b", minority.URL),
		mkConsensusUpstream("c", majority.URL),
	}

	for i := 0; i < 20; i++ {
		resp, _, err := cp.Execute(context.Background(), ups, func(u *upstream.Upstream) (*http.Request, error) {
			return http.NewRequest(http.MethodGet, u.URL, nil)
		})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "majority") {
			t.Fatalf("iteration %d: minority group won: %s", i, body)
		}
	}
}

func TestConsensusPolicy_ServerErrorOpensBreaker(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	u := upstream.New("testnet", config.UpstreamConfig{ID: "a", URL: srv.URL},
		&config.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, HalfOpenAfter: time.Hour}, 100)
	cp := &ConsensusPolicy{MaxParticipants: 1, AgreementThreshold: 1}

	_, _, _ = cp.Execute(context.Background(), []*upstream.Upstream{u}, func(u *upstream.Upstream) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, u.URL, nil)
	})

	if u.IsReady() {
		t.Fatal("a 5xx consensus response must count as a circuit-breaker failure")
	}
}

func TestConsensusPolicy_GatedParticipantsReportCircuitUnavailable(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":"ok"}`))
	}))
	defer srv.Close()

	cb := &config.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, HalfOpenAfter: time.Millisecond}
	ups := make([]*upstream.Upstream, 3)
	for i := range ups {
		ups[i] = upstream.New("testnet", config.UpstreamConfig{ID: fmt.Sprintf("u%d", i), URL: srv.URL}, cb, 100)
	}

	// Hold the recovery probe on two of the three participants, as a
	// concurrent request would, leaving too few to reach agreement.
	for _, u := range ups[:2] {
		u.CBFailure()
	}
	time.Sleep(2 * time.Millisecond)
	for _, u := range ups[:2] {
		if _, ok := u.CBTryAcquire(); !ok {
			t.Fatal("failed to hold the recovery probe")
		}
	}

	cp := &ConsensusPolicy{MaxParticipants: 3, AgreementThreshold: 2}
	_, _, err := cp.Execute(context.Background(), ups, func(u *upstream.Upstream) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, u.URL, nil)
	})
	if !errors.Is(err, upstream.ErrCircuitUnavailable) {
		t.Fatalf("gated participants must surface ErrCircuitUnavailable so the caller can fall back, got %v", err)
	}
}
