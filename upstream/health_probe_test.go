package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
)

func newProbePool(t *testing.T, upstreamCfg config.UpstreamConfig) *Pool {
	t.Helper()
	pool, err := NewPool(
		"probe-net",
		[]config.UpstreamConfig{upstreamCfg},
		config.RoutingConfig{LoadBalancing: "round-robin"},
		config.HealthConfig{
			CheckInterval:    time.Minute,
			FinalityInterval: time.Minute,
			MaxSyncDistance:  10,
			FollowDistance:   32,
			MaxHeadDistance:  2,
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestCheckSync_SendsConfiguredHeaders(t *testing.T) {
	t.Parallel()
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"head_slot":"100","sync_distance":"0","is_syncing":false,"el_offline":false}}`))
	}))
	defer srv.Close()

	pool := newProbePool(t, config.UpstreamConfig{
		ID:      "a",
		URL:     srv.URL,
		Headers: map[string]string{"Authorization": "Bearer probe-token"},
	})
	u := pool.ByID("a")
	pool.monitor.checkSync(context.Background(), u)

	if gotAuth != "Bearer probe-token" {
		t.Fatalf("probe did not send configured headers, got Authorization=%q", gotAuth)
	}
	if !u.IsHealthy() {
		t.Fatal("upstream should be healthy after successful probe")
	}
}

func TestCheckSync_WrongShapeResponseMarksDown(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"message":"backend unavailable"}`))
	}))
	defer srv.Close()

	pool := newProbePool(t, config.UpstreamConfig{ID: "a", URL: srv.URL})
	u := pool.ByID("a")
	if !u.IsHealthy() {
		t.Fatal("upstream should start healthy")
	}
	pool.monitor.checkSync(context.Background(), u)

	if u.IsHealthy() {
		t.Fatal("a 200 response of the wrong JSON shape must mark the upstream down, not up")
	}
}
