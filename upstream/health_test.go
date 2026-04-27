package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
)

func testMonitor(t *testing.T, upstreamURL string) (*HealthMonitor, *Upstream) {
	t.Helper()
	u := New("net", config.UpstreamConfig{ID: "u1", URL: upstreamURL}, nil, 0)
	p := &Pool{networkID: "net", upstreams: []*Upstream{u}, blockCache: NewBlockCache(32, 2)}
	h := newHealthMonitor([]*Upstream{u}, p, config.HealthConfig{
		CheckInterval: time.Minute, FinalityInterval: time.Minute,
		MaxSyncDistance: 10, FollowDistance: 32, MaxHeadDistance: 2,
	})
	return h, u
}

func TestCheckNodeHealth_200_NoChange(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	u.SetHealth(HealthDown)
	before := u.ScoreSnapshot()
	h.checkNodeHealth(context.Background(), u)
	if u.Health() != HealthDown {
		t.Fatalf("expected HealthDown, got %v", u.Health())
	}
	after := u.ScoreSnapshot()
	if after.Samples != before.Samples+1 {
		t.Fatalf("expected health probe to add one score sample, got before=%d after=%d", before.Samples, after.Samples)
	}
	if after.ErrorRate != 0 {
		t.Fatalf("expected successful health probe to keep error rate at 0, got %f", after.ErrorRate)
	}
}

func TestCheckNodeHealth_206_Degraded(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPartialContent)
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	u.SetHealth(HealthUp)
	before := u.ScoreSnapshot()
	h.checkNodeHealth(context.Background(), u)
	if u.Health() != HealthDegraded {
		t.Fatalf("expected HealthDegraded, got %v", u.Health())
	}
	after := u.ScoreSnapshot()
	if after.Samples != before.Samples+1 {
		t.Fatalf("expected degraded health probe to add one score sample, got before=%d after=%d", before.Samples, after.Samples)
	}
	if after.ErrorRate != 0 {
		t.Fatalf("expected 206 node health probe not to count as an error, got %f", after.ErrorRate)
	}
}

func TestCheckNodeHealth_503_Down(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	u.SetHealth(HealthUp)
	before := u.ScoreSnapshot()
	h.checkNodeHealth(context.Background(), u)
	if u.Health() != HealthDown {
		t.Fatalf("expected HealthDown, got %v", u.Health())
	}
	after := u.ScoreSnapshot()
	if after.Samples != before.Samples+1 {
		t.Fatalf("expected failing health probe to add one score sample, got before=%d after=%d", before.Samples, after.Samples)
	}
	if after.ErrorRate <= before.ErrorRate {
		t.Fatalf("expected failing health probe to increase error rate, got before=%f after=%f", before.ErrorRate, after.ErrorRate)
	}
}

func TestCheckNodeHealth_ConnError_Down(t *testing.T) {
	t.Parallel()
	h, u := testMonitor(t, "http://127.0.0.1:1")
	u.SetHealth(HealthUp)
	before := u.ScoreSnapshot()
	h.checkNodeHealth(context.Background(), u)
	if u.Health() != HealthDown {
		t.Fatalf("expected HealthDown, got %v", u.Health())
	}
	after := u.ScoreSnapshot()
	if after.Samples != before.Samples+1 {
		t.Fatalf("expected connection-error health probe to add one score sample, got before=%d after=%d", before.Samples, after.Samples)
	}
	if after.ErrorRate <= before.ErrorRate {
		t.Fatalf("expected connection-error health probe to increase error rate, got before=%f after=%f", before.ErrorRate, after.ErrorRate)
	}
}
