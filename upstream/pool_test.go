package upstream

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ebeacon/ebeacon/config"
)

var poolLabelSeq atomic.Uint64

func TestPool_Select_Ready(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "a", URL: "http://127.0.0.1:1"},
		{ID: "b", URL: "http://127.0.0.1:2"},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}
	ups := p.Select(2)
	if len(ups) != 2 {
		t.Fatalf("Select: got %d upstreams", len(ups))
	}
}

func TestPool_Select_PriorityRoundRobin(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool("test", []config.UpstreamConfig{
		{ID: "p0_a", URL: "http://a", Priority: 0},
		{ID: "p0_b", URL: "http://b", Priority: 0},
		{ID: "p1_c", URL: "http://c", Priority: 1},
		{ID: "p1_d", URL: "http://d", Priority: 1},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		ups := p.Select(4)
		if len(ups) != 4 {
			t.Fatalf("expected 4")
		}
		// Priority 0 elements should always be in the first 2 positions
		if (ups[0].ID != "p0_a" && ups[0].ID != "p0_b") || (ups[1].ID != "p0_a" && ups[1].ID != "p0_b") {
			t.Fatalf("Priority breached: %v", ups)
		}
	}
}

func TestPool_Select_WeightedRoundRobin(t *testing.T) {
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "heavy", URL: "http://a", Weight: 3},
		{ID: "light", URL: "http://b", Weight: 1},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	counts := map[string]int{}
	for i := 0; i < 40; i++ {
		ups := p.Select(1)
		counts[ups[0].ID]++
	}
	if counts["heavy"] != 30 || counts["light"] != 10 {
		t.Fatalf("unexpected weighted round-robin counts: %+v", counts)
	}
}

func TestPool_Select_WeightedRandom(t *testing.T) {
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "heavy", URL: "http://a", Weight: 4},
		{ID: "light", URL: "http://b", Weight: 1},
	}, config.RoutingConfig{LoadBalancing: "random"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	counts := map[string]int{}
	for i := 0; i < 500; i++ {
		ups := p.Select(1)
		counts[ups[0].ID]++
	}
	if counts["heavy"] <= counts["light"] {
		t.Fatalf("expected higher-weight random upstream to win more often: %+v", counts)
	}
	if counts["heavy"] < 300 {
		t.Fatalf("expected noticeable weight bias in random mode: %+v", counts)
	}
}

func TestPool_Select_WeightedLeastConn(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "small", URL: "http://a", Weight: 1},
		{ID: "large", URL: "http://b", Weight: 3},
	}, config.RoutingConfig{LoadBalancing: "least-conn"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	small := p.ByID("small")
	large := p.ByID("large")
	small.IncrActive()
	large.IncrActive()
	large.IncrActive()

	ups := p.Select(2)
	if len(ups) != 2 {
		t.Fatalf("expected 2 upstreams, got %d", len(ups))
	}
	if ups[0].ID != "large" {
		t.Fatalf("expected weighted least-conn to prefer large, got %s", ups[0].ID)
	}
}

func TestPool_Select_WeightedScore(t *testing.T) {
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "heavy", URL: "http://a", Weight: 4},
		{ID: "light", URL: "http://b", Weight: 1},
	}, config.RoutingConfig{LoadBalancing: "score"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	heavy := p.ByID("heavy")
	light := p.ByID("light")
	if heavy == nil || light == nil {
		t.Fatal("expected score test upstreams")
	}
	heavy.UpdateSyncStatus(false, 128, 0)
	light.UpdateSyncStatus(false, 128, 0)
	heavy.RecordSuccess(25 * time.Millisecond)
	light.RecordSuccess(25 * time.Millisecond)
	p.BlockCache().AddBlock("heavy", 128, "0x1", "0x0")
	p.BlockCache().AddBlock("light", 128, "0x1", "0x0")

	counts := map[string]int{}
	for i := 0; i < 500; i++ {
		ups := p.Select(1)
		counts[ups[0].ID]++
	}
	if counts["heavy"] <= counts["light"] {
		t.Fatalf("expected heavier score-weighted upstream to win more often: %+v", counts)
	}
	if counts["heavy"] < 300 {
		t.Fatalf("expected noticeable weight bias in score mode: %+v", counts)
	}
}

func TestPool_NodeHealthStatusForSelector(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "lighthouse-a", URL: "http://a"},
		{ID: "teku-1", URL: "http://b"},
		{ID: "archive", URL: "http://c"},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	for _, u := range p.All() {
		switch u.ID {
		case "lighthouse-a":
			u.SetClientType(ClientLighthouse)
			u.SetHealth(HealthDegraded)
		case "teku-1":
			u.SetClientType(ClientTeku)
			u.SetHealth(HealthUp)
		case "archive":
			u.SetClientType(ClientLighthouse)
			u.SetHealth(HealthDown)
		}
	}

	tests := []struct {
		name     string
		selector string
		want     HealthStatus
	}{
		{name: "exact upstream", selector: "archive", want: HealthDown},
		{name: "client type", selector: "client:lighthouse", want: HealthDegraded},
		{name: "glob", selector: "*teku*", want: HealthUp},
		{name: "no matches", selector: "missing-*", want: HealthDown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := p.NodeHealthStatusForSelector(tt.selector); got != tt.want {
				t.Fatalf("selector %q: got %v want %v", tt.selector, got, tt.want)
			}
		})
	}
}
