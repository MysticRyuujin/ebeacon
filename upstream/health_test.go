package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

func TestCheckNodeHealth_404DoesNotLatchDown(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/node/health":
			w.WriteHeader(http.StatusNotFound) // gateway doesn't proxy /health
		case "/eth/v1/node/syncing":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"head_slot":"100","sync_distance":"0","is_syncing":false}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	h.checkNodeHealth(context.Background(), u)
	// A healthy /syncing poll must be able to bring the node Up — a 404 on
	// /health is not a health verdict and must not latch it down.
	h.checkSync(context.Background(), u)
	if u.Health() != HealthUp {
		t.Fatalf("404 on /health must not exclude an otherwise-healthy node, got %v", u.Health())
	}
}

func TestCheckNodeHealth_429DoesNotLatchDown(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/node/health":
			w.WriteHeader(http.StatusTooManyRequests)
		case "/eth/v1/node/syncing":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"head_slot":"100","sync_distance":"0","is_syncing":false}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	h.checkNodeHealth(context.Background(), u)
	h.checkSync(context.Background(), u)
	if u.Health() != HealthUp {
		t.Fatalf("429 on /health must not exclude an otherwise-healthy node, got %v", u.Health())
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

func TestCheckNodeHealth_503LatchesThroughSyncPoll(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/node/health":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/eth/v1/node/syncing":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"head_slot":"100","sync_distance":"0","is_syncing":false}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	h.checkNodeHealth(context.Background(), u)
	if u.Health() != HealthDown {
		t.Fatalf("503 must mark down, got %v", u.Health())
	}

	// A healthy /syncing poll must not flap the upstream back Up while the
	// node-health probe still reports 503.
	h.checkSync(context.Background(), u)
	if u.Health() != HealthDown {
		t.Fatalf("healthy sync poll must not override latched node-health 503, got %v", u.Health())
	}
}

func TestCheckNodeHealth_RecoveryClearsLatch(t *testing.T) {
	t.Parallel()
	var healthStatus atomic.Int32
	healthStatus.Store(http.StatusServiceUnavailable)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/node/health":
			w.WriteHeader(int(healthStatus.Load()))
		case "/eth/v1/node/syncing":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"head_slot":"100","sync_distance":"0","is_syncing":false}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	h.checkNodeHealth(context.Background(), u)
	healthStatus.Store(http.StatusOK)
	h.checkNodeHealth(context.Background(), u)
	h.checkSync(context.Background(), u)
	if u.Health() != HealthUp {
		t.Fatalf("recovered node must return to Up, got %v", u.Health())
	}
}

func TestCheckSync_ElOfflineDegraded(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"head_slot":"100","sync_distance":"0","is_syncing":false,"el_offline":true}}`))
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	h.checkSync(context.Background(), u)
	if u.Health() != HealthDegraded {
		t.Fatalf("el_offline must degrade the upstream, got %v", u.Health())
	}
}

func syncedOrHealthHandler(healthStatus, syncStatus *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/node/health":
			w.WriteHeader(int(healthStatus.Load()))
		case "/eth/v1/node/syncing":
			if s := int(syncStatus.Load()); s != http.StatusOK {
				w.WriteHeader(s)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"head_slot":"100","sync_distance":"0","is_syncing":false,"el_offline":false}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func TestHealth_NoFlapBetween206AndHealthySync(t *testing.T) {
	t.Parallel()
	var healthStatus, syncStatus atomic.Int64
	healthStatus.Store(http.StatusPartialContent)
	syncStatus.Store(http.StatusOK)
	srv := httptest.NewServer(syncedOrHealthHandler(&healthStatus, &syncStatus))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	for i := 0; i < 3; i++ {
		h.checkNodeHealth(context.Background(), u)
		if u.Health() != HealthDegraded {
			t.Fatalf("cycle %d after node-health probe: got %v want HealthDegraded", i, u.Health())
		}
		h.checkSync(context.Background(), u)
		if u.Health() != HealthDegraded {
			t.Fatalf("cycle %d after sync probe: got %v want HealthDegraded (flap)", i, u.Health())
		}
	}
}

func TestHealth_NoFlapBetween206AndSyncError(t *testing.T) {
	t.Parallel()
	var healthStatus, syncStatus atomic.Int64
	healthStatus.Store(http.StatusPartialContent)
	syncStatus.Store(http.StatusInternalServerError)
	srv := httptest.NewServer(syncedOrHealthHandler(&healthStatus, &syncStatus))
	defer srv.Close()

	for name, first := range map[string]func(*HealthMonitor, *Upstream){
		"sync-first": func(h *HealthMonitor, u *Upstream) { h.checkSync(context.Background(), u) },
		"node-first": func(h *HealthMonitor, u *Upstream) { h.checkNodeHealth(context.Background(), u) },
	} {
		t.Run(name, func(t *testing.T) {
			h, u := testMonitor(t, srv.URL)
			first(h, u)
			h.checkSync(context.Background(), u)
			for i := 0; i < 3; i++ {
				h.checkNodeHealth(context.Background(), u)
				if got := u.Health(); got != HealthDown {
					t.Fatalf("cycle %d after node-health probe: got %v want HealthDown", i, got)
				}
				h.checkSync(context.Background(), u)
				if got := u.Health(); got != HealthDown {
					t.Fatalf("cycle %d after sync probe: got %v want HealthDown", i, got)
				}
			}
		})
	}
}

func TestHealth_NodeVerdictClearLiftsImmediately(t *testing.T) {
	t.Parallel()
	var healthStatus, syncStatus atomic.Int64
	healthStatus.Store(http.StatusPartialContent)
	syncStatus.Store(http.StatusOK)
	srv := httptest.NewServer(syncedOrHealthHandler(&healthStatus, &syncStatus))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	h.checkNodeHealth(context.Background(), u)
	h.checkSync(context.Background(), u)
	if u.Health() != HealthDegraded {
		t.Fatalf("setup: got %v want HealthDegraded", u.Health())
	}

	healthStatus.Store(http.StatusOK)
	h.checkNodeHealth(context.Background(), u)
	if u.Health() != HealthUp {
		t.Fatalf("cleared node verdict must lift health without waiting for a sync poll, got %v", u.Health())
	}
}

func TestSetHealth_OverridesNodeVerdict(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	h, u := testMonitor(t, srv.URL)
	h.checkNodeHealth(context.Background(), u)
	if u.Health() != HealthDown {
		t.Fatalf("setup: got %v want HealthDown", u.Health())
	}

	u.SetHealth(HealthUp)
	if u.Health() != HealthUp {
		t.Fatalf("SetHealth must override the latched node verdict, got %v", u.Health())
	}
}
