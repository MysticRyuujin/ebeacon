package network

import (
	"regexp"
	"testing"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/upstream"
)

func TestCompiledRouting_applyClientPrefix_BeaconAPI(t *testing.T) {
	t.Parallel()
	r := &compiledRouting{
		clientRoutes: []compiledClientRoute{
			{prefix: "/lighthouse/", upstreamID: "lighthouse"},
		},
	}
	path, up := r.applyClientPrefix("/lighthouse/eth/v1/node/version")
	if path != "/eth/v1/node/version" || up != "lighthouse" {
		t.Fatalf("got path=%q upstream=%q", path, up)
	}
}

func TestCompiledRouting_applyClientPrefix_NonStandardKeepsPath(t *testing.T) {
	t.Parallel()
	r := &compiledRouting{
		clientRoutes: []compiledClientRoute{
			{prefix: "/lighthouse/", upstreamID: "lighthouse"},
		},
	}
	path, up := r.applyClientPrefix("/lighthouse/custom/foo")
	if path != "/lighthouse/custom/foo" || up != "lighthouse" {
		t.Fatalf("got path=%q upstream=%q", path, up)
	}
}

type inferSelectorPoolStub struct {
	byID     map[string]*upstream.Upstream
	byClient map[string][]*upstream.Upstream
}

func (p inferSelectorPoolStub) ByID(id string) *upstream.Upstream {
	return p.byID[id]
}

func (p inferSelectorPoolStub) SelectByClientType(clientType string, n int) []*upstream.Upstream {
	ups := p.byClient[clientType]
	if len(ups) > n {
		return ups[:n]
	}
	return ups
}

func TestInferClientSelectorPath_ClientType(t *testing.T) {
	t.Parallel()
	pool := inferSelectorPoolStub{
		byClient: map[string][]*upstream.Upstream{
			upstream.ClientNimbus: {{ID: "nimbus-1"}},
		},
	}
	path, up, matched := inferClientSelectorPath("/nimbus/eth/v1/node/version", pool)
	if !matched || path != "/eth/v1/node/version" || up != "client:nimbus" {
		t.Fatalf("matched=%v path=%q upstream=%q", matched, path, up)
	}
}

func TestInferClientSelectorPath_ExactUpstreamID(t *testing.T) {
	t.Parallel()
	pool := inferSelectorPoolStub{
		byID: map[string]*upstream.Upstream{
			"archive": {ID: "archive"},
		},
	}
	path, up, matched := inferClientSelectorPath("/archive/eth/v1/node/version", pool)
	if !matched || path != "/eth/v1/node/version" || up != "archive" {
		t.Fatalf("matched=%v path=%q upstream=%q", matched, path, up)
	}
}

func TestInferClientSelectorPath_NonBeaconPathIgnored(t *testing.T) {
	t.Parallel()
	pool := inferSelectorPoolStub{
		byID: map[string]*upstream.Upstream{
			"nimbus": {ID: "nimbus"},
		},
	}
	path, up, matched := inferClientSelectorPath("/nimbus/custom/foo", pool)
	if matched || path != "/nimbus/custom/foo" || up != "" {
		t.Fatalf("matched=%v path=%q upstream=%q", matched, path, up)
	}
}

func TestCompiledRouting_matchRouteRule_Deny(t *testing.T) {
	t.Parallel()
	cr := &compiledRouting{
		routeRules: []compiledRouteRule{
			{re: regexp.MustCompile(`^/eth/v1/debug/`), deny: true},
		},
	}
	deny, _, hit := cr.matchRouteRule("GET", "/eth/v1/debug/foo")
	if !hit || !deny {
		t.Fatalf("expected deny, hit=%v deny=%v", hit, deny)
	}
}

func TestCompiledRouting_matchRouteRule_MethodFilter(t *testing.T) {
	t.Parallel()
	cr := &compiledRouting{
		routeRules: []compiledRouteRule{
			{
				re:         regexp.MustCompile(`^/eth/v1/node/version$`),
				methods:    map[string]bool{"POST": true},
				upstreamID: "teku",
			},
		},
	}
	_, _, hit := cr.matchRouteRule("GET", "/eth/v1/node/version")
	if hit {
		t.Fatal("GET should not match POST-only rule")
	}
	deny, up, hit := cr.matchRouteRule("POST", "/eth/v1/node/version")
	if !hit || deny || up != "teku" {
		t.Fatalf("POST match: deny=%v up=%q hit=%v", deny, up, hit)
	}
}

func TestCompileRouting_FromConfig(t *testing.T) {
	t.Parallel()
	cr, err := compileRouting(&config.RoutingConfig{
		ClientRoutes: []config.ClientRouteConfig{
			{PathPrefix: "/teku/", UpstreamID: "teku"},
		},
		RouteRules: []config.RouteRuleConfig{
			{PathPattern: `^/eth/v1/beacon/blocks$`, Methods: []string{"POST"}, UpstreamID: "teku", Deny: false},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cr == nil {
		t.Fatal("expected non-nil compiled routing")
	}
	if len(cr.clientRoutes) != 1 || len(cr.routeRules) != 1 {
		t.Fatalf("routes: client=%d rules=%d", len(cr.clientRoutes), len(cr.routeRules))
	}
}
