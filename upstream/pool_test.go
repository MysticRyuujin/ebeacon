package upstream

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
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

func TestPool_SelectForPathPreferCanonicalHead_PrefersCanonicalReporters(t *testing.T) {
	// MaxHeadDistance=4 so upstreams one slot behind the canonical head are
	// still considered "on the canonical fork" and remain in the candidate
	// pool. This mirrors the real race scenario: "fast" is a valid candidate
	// by the existing fork filter and wins score-based routing, even though
	// it has not actually reported the new head block.
	health := config.HealthConfig{MaxSyncDistance: 10, MaxHeadDistance: 4}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "fast", URL: "http://a", Weight: 1},
		{ID: "slow-canon", URL: "http://b", Weight: 1},
		{ID: "other", URL: "http://c", Weight: 1},
	}, config.RoutingConfig{LoadBalancing: "score"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	fast := p.ByID("fast")
	slowCanon := p.ByID("slow-canon")
	other := p.ByID("other")
	fast.UpdateSyncStatus(false, 128, 0)
	slowCanon.UpdateSyncStatus(false, 128, 0)
	other.UpdateSyncStatus(false, 128, 0)

	// Feed score samples: fast is much faster.
	for range 10 {
		fast.RecordSuccess(5 * time.Millisecond)
		slowCanon.RecordSuccess(200 * time.Millisecond)
		other.RecordSuccess(200 * time.Millisecond)
	}

	// All three agreed on slot 127, but only slow-canon has observed slot 128
	// (the new canonical head). "fast" and "other" are still one slot behind —
	// they satisfy IsOnCanonicalFork (within maxHeadDistance) but they have
	// not reported the canonical head block itself.
	p.BlockCache().AddBlock("fast", 127, "0xprev", "0x0")
	p.BlockCache().AddBlock("slow-canon", 127, "0xprev", "0x0")
	p.BlockCache().AddBlock("other", 127, "0xprev", "0x0")
	p.BlockCache().AddBlock("slow-canon", 128, "0xhead", "0xprev")

	// Sanity: without the head-aware filter, all three upstreams remain in
	// play under score-based routing (fast is still a valid candidate
	// because it is within maxHeadDistance of the canonical head). If the
	// default selector happened to already lock onto slow-canon, the rest
	// of this test would prove nothing.
	seenDefault := map[string]bool{}
	for range 300 {
		ups := p.SelectForPath("/eth/v1/beacon/headers/{block_id}", 1)
		seenDefault[ups[0].ID] = true
	}
	if !seenDefault["fast"] {
		t.Fatalf("sanity: expected default score ordering to route to fast at least once, got %v", seenDefault)
	}

	// SelectForPathPreferCanonicalHead hard-prefers the head reporter, so
	// slow-canon must win *every* time even though its score is worse.
	for range 200 {
		ups := p.SelectForPathPreferCanonicalHead("/eth/v1/beacon/headers/{block_id}", 1)
		if ups[0].ID != "slow-canon" {
			t.Fatalf("expected slow-canon to win head-aware selection, got %s", ups[0].ID)
		}
	}
}

func TestPool_SelectForPathPreferCanonicalHead_FailsOpenBeforeHeadSeen(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "a", URL: "http://a"},
		{ID: "b", URL: "http://b"},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range p.All() {
		u.SetHealth(HealthUp)
	}

	// No blocks recorded yet — CanonicalHeadSeenBy returns nil. The selector
	// must fall open and return the normal candidate set rather than nothing.
	ups := p.SelectForPathPreferCanonicalHead("/eth/v1/beacon/headers/head", 2)
	if len(ups) != 2 {
		t.Fatalf("expected fail-open to return both upstreams, got %d", len(ups))
	}
}

func TestPool_SelectForPathPreferCanonicalHead_FailsOpenWhenOnlyForkedHasHead(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "canon-a", URL: "http://a"},
		{ID: "canon-b", URL: "http://b"},
		{ID: "forked", URL: "http://c"},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range p.All() {
		u.UpdateSyncStatus(false, 128, 0)
	}

	// canon-a and canon-b are on the canonical chain at slot 128 (which is the
	// canonical head by majority vote). "forked" reports a different block at
	// slot 128 — it's on a competing fork and the normal canonical-fork filter
	// excludes it. The head-aware filter must not accidentally resurrect it.
	p.BlockCache().AddBlock("canon-a", 128, "0xcanon", "0x0")
	p.BlockCache().AddBlock("canon-b", 128, "0xcanon", "0x0")
	p.BlockCache().AddBlock("forked", 128, "0xfork", "0x0")

	for range 50 {
		ups := p.SelectForPathPreferCanonicalHead("/eth/v1/beacon/headers/head", 3)
		for _, u := range ups {
			if u.ID == "forked" {
				t.Fatalf("forked upstream must not appear in head-aware selection: %v", ups)
			}
		}
	}
}

func TestPool_SelectForPathArchive_FiltersToArchiveOnly(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "pruned-a", URL: "http://a", Priority: 0, Archive: false},
		{ID: "pruned-b", URL: "http://b", Priority: 0, Archive: false},
		{ID: "archive-local", URL: "http://c", Priority: 0, Archive: true},
		{ID: "archive-cloud", URL: "http://d", Priority: 10, Archive: true},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	ups := p.SelectForPathArchive("/eth/v2/beacon/blocks/12345", 4)
	if len(ups) != 2 {
		t.Fatalf("expected 2 archive-capable upstreams, got %d: %+v", len(ups), upstreamIDs(ups))
	}
	for _, u := range ups {
		if !u.IsArchive() {
			t.Fatalf("non-archive upstream %q returned from SelectForPathArchive", u.ID)
		}
	}
	// Priority within archive set: local (priority 0) must come before cloud (priority 10)
	if ups[0].ID != "archive-local" {
		t.Fatalf("priority-0 archive upstream should be preferred, got %q first", ups[0].ID)
	}
}

func TestPool_SelectForPathArchive_NoArchiveReturnsNil(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "pruned-a", URL: "http://a"},
		{ID: "pruned-b", URL: "http://b"},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	if got := p.SelectForPathArchive("/eth/v2/beacon/blocks/12345", 2); got != nil {
		t.Fatalf("expected nil when no archive upstreams configured, got %+v", upstreamIDs(got))
	}
	if p.HasArchive() {
		t.Fatalf("HasArchive should be false when no archive upstreams configured")
	}
}

func TestPool_HasArchive(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10}
	cb := &config.CircuitBreakerConfig{FailureThreshold: 5, SuccessThreshold: 2, HalfOpenAfter: 30 * time.Second}
	p, err := NewPool(fmt.Sprintf("pool_%d_%s", poolLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_")), []config.UpstreamConfig{
		{ID: "pruned", URL: "http://a"},
		{ID: "archive", URL: "http://b", Archive: true},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}
	if !p.HasArchive() {
		t.Fatalf("HasArchive should be true when at least one archive upstream exists")
	}
}

func upstreamIDs(ups []*Upstream) []string {
	ids := make([]string, len(ups))
	for i, u := range ups {
		ids[i] = u.ID
	}
	return ids
}

func TestPool_GetForPath_PreferredForkedUpstreamNotReturned(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10, FollowDistance: 32, MaxHeadDistance: 2}
	cb := &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    30 * time.Second,
	}
	p, err := NewPool("test-fork", []config.UpstreamConfig{
		{ID: "a", URL: "http://127.0.0.1:1"},
		{ID: "b", URL: "http://127.0.0.1:2"},
		{ID: "c", URL: "http://127.0.0.1:3"},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	// With no fork data, the preferred upstream is honored (fail-open).
	u, err := p.GetForPath("c", "")
	if err != nil || u.ID != "c" {
		t.Fatalf("fail-open preferred pick: got %v, %v", u, err)
	}

	// Majority sees root-canon at slot 100; c reports a competing head.
	p.BlockCache().AddBlock("a", 100, "root-canon", "p99")
	p.BlockCache().AddBlock("b", 100, "root-canon", "p99")
	p.BlockCache().AddBlock("c", 100, "root-fork", "p99")

	u, err = p.GetForPath("c", "")
	if err != nil {
		t.Fatal(err)
	}
	if u.ID == "c" {
		t.Fatal("sticky/preferred routing must not return a forked upstream")
	}
}

func TestPool_GetForPath_LaggingPreferredUpstreamKeptSticky(t *testing.T) {
	t.Parallel()
	health := config.HealthConfig{MaxSyncDistance: 10, FollowDistance: 32, MaxHeadDistance: 2}
	cb := &config.CircuitBreakerConfig{FailureThreshold: 5, SuccessThreshold: 2, HalfOpenAfter: 30 * time.Second}
	p, err := NewPool("test-lag", []config.UpstreamConfig{
		{ID: "a", URL: "http://127.0.0.1:1"},
		{ID: "b", URL: "http://127.0.0.1:2"},
		{ID: "c", URL: "http://127.0.0.1:3"},
	}, config.RoutingConfig{LoadBalancing: "round-robin"}, health, cb)
	if err != nil {
		t.Fatal(err)
	}

	// a/b advance to slot 105; c last reported slot 100 — behind by more than
	// maxHeadDistance (2), so it is lagging but NOT on a competing fork.
	for slot := uint64(100); slot <= 105; slot++ {
		root := fmt.Sprintf("root-%d", slot)
		p.BlockCache().AddBlock("a", slot, root, "p")
		p.BlockCache().AddBlock("b", slot, root, "p")
	}
	p.BlockCache().AddBlock("c", 100, "root-100", "p")

	if got := p.BlockCache().ForkStatus("c"); got != "lagging" {
		t.Fatalf("precondition: c should be lagging, got %q", got)
	}
	u, err := p.GetForPath("c", "")
	if err != nil {
		t.Fatal(err)
	}
	if u.ID != "c" {
		t.Fatalf("a merely-lagging preferred upstream must keep its sticky slot, got %q", u.ID)
	}
}
