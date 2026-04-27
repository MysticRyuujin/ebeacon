package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mysticryuujin/ebeacon/config"
	networkpkg "github.com/mysticryuujin/ebeacon/network"
	"github.com/mysticryuujin/ebeacon/upstream"
)

var proxyLabelSeq atomic.Uint64

func networkID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("net_%d_%s", proxyLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_"))
}

// uniquePair returns two distinct network IDs so Prometheus metrics do not collide across tests.
func uniquePair(t *testing.T) (a, b string) {
	t.Helper()
	n := proxyLabelSeq.Add(1)
	base := fmt.Sprintf("%d_%s", n, strings.ReplaceAll(t.Name(), "/", "_"))
	return "a_" + base, "b_" + base
}

func mustLoadConfig(t *testing.T, netID, upstreamURL string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	yaml := fmt.Sprintf(`logLevel: error
server:
  host: "127.0.0.1"
  port: 5555
  maxTimeout: 30s
failsafe:
  timeout:
    duration: 10s
health:
  checkInterval: 1h
  finalityInterval: 1h
  maxSyncDistance: 10
rateLimiting: {}
metrics:
  enabled: false
networks:
  - id: %s
    upstreams:
      - id: u1
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
    cache:
      enabled: false
`, netID, upstreamURL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

func TestProxy_SingleNetwork_PrefixFree(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v1/node/version" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":"from-mock"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer up.Close()

	netID := networkID(t)
	cfg := mustLoadConfig(t, netID, up.URL)
	n, err := networkpkg.New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := New(map[string]*networkpkg.Network{netID: n})

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "from-mock") {
		t.Fatalf("body: %q", rec.Body.String())
	}
}

func TestProxy_MultiNetwork_Prefix(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v1/node/version" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":"m"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer up.Close()

	idA, idB := uniquePair(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	yaml := fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams: [{ id: u1, url: %q }]
    routing: { loadBalancing: round-robin, stickySession: false }
    cache: { enabled: false }
  - id: %s
    upstreams: [{ id: u2, url: %q }]
    routing: { loadBalancing: round-robin, stickySession: false }
    cache: { enabled: false }
`, idA, up.URL, idB, up.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}

	na, err := networkpkg.New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	nb, err := networkpkg.New(&cfg.Networks[1], cfg)
	if err != nil {
		t.Fatal(err)
	}

	p := New(map[string]*networkpkg.Network{
		idA: na,
		idB: nb,
	})

	req := httptest.NewRequest(http.MethodGet, "/"+idA+"/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first network: status %d %q", rec.Code, rec.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodGet, "/"+idB+"/eth/v1/node/version", nil)
	rec2 := httptest.NewRecorder()
	p.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second network: status %d %q", rec2.Code, rec2.Body.String())
	}
}

func TestProxy_MultiNetwork_CacheIsolation(t *testing.T) {
	var mainnetHits atomic.Int32
	mainnetUp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/node/version" {
			http.NotFound(w, r)
			return
		}
		mainnetHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"version":"from-mainnet"}}`))
	}))
	defer mainnetUp.Close()

	var sepoliaHits atomic.Int32
	sepoliaUp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/node/version" {
			http.NotFound(w, r)
			return
		}
		sepoliaHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"version":"from-sepolia"}}`))
	}))
	defer sepoliaUp.Close()

	mainnetID, sepoliaID := uniquePair(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	yaml := fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams: [{ id: u1, url: %q }]
    routing: { loadBalancing: round-robin, stickySession: false }
    cache: { enabled: true }
  - id: %s
    upstreams: [{ id: u2, url: %q }]
    routing: { loadBalancing: round-robin, stickySession: false }
    cache: { enabled: true }
`, mainnetID, mainnetUp.URL, sepoliaID, sepoliaUp.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}

	mainnetNet, err := networkpkg.New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	sepoliaNet, err := networkpkg.New(&cfg.Networks[1], cfg)
	if err != nil {
		t.Fatal(err)
	}

	p := New(map[string]*networkpkg.Network{
		mainnetID: mainnetNet,
		sepoliaID: sepoliaNet,
	})

	firstMainnet := httptest.NewRequest(http.MethodGet, "/"+mainnetID+"/eth/v1/node/version", nil)
	firstMainnetRec := httptest.NewRecorder()
	p.ServeHTTP(firstMainnetRec, firstMainnet)
	if firstMainnetRec.Code != http.StatusOK {
		t.Fatalf("first mainnet request: status %d body %q", firstMainnetRec.Code, firstMainnetRec.Body.String())
	}
	if !strings.Contains(firstMainnetRec.Body.String(), "from-mainnet") {
		t.Fatalf("first mainnet body: %q", firstMainnetRec.Body.String())
	}
	if got := firstMainnetRec.Header().Get("X-Ebeacon-Cache"); got == "HIT" {
		t.Fatalf("first mainnet request unexpectedly served from cache")
	}

	secondMainnet := httptest.NewRequest(http.MethodGet, "/"+mainnetID+"/eth/v1/node/version", nil)
	secondMainnetRec := httptest.NewRecorder()
	p.ServeHTTP(secondMainnetRec, secondMainnet)
	if secondMainnetRec.Code != http.StatusOK {
		t.Fatalf("second mainnet request: status %d body %q", secondMainnetRec.Code, secondMainnetRec.Body.String())
	}
	if got := secondMainnetRec.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("second mainnet cache header: got %q want HIT", got)
	}
	if !strings.Contains(secondMainnetRec.Body.String(), "from-mainnet") {
		t.Fatalf("second mainnet body: %q", secondMainnetRec.Body.String())
	}

	firstSepolia := httptest.NewRequest(http.MethodGet, "/"+sepoliaID+"/eth/v1/node/version", nil)
	firstSepoliaRec := httptest.NewRecorder()
	p.ServeHTTP(firstSepoliaRec, firstSepolia)
	if firstSepoliaRec.Code != http.StatusOK {
		t.Fatalf("first sepolia request: status %d body %q", firstSepoliaRec.Code, firstSepoliaRec.Body.String())
	}
	if !strings.Contains(firstSepoliaRec.Body.String(), "from-sepolia") {
		t.Fatalf("first sepolia body: %q", firstSepoliaRec.Body.String())
	}
	if got := firstSepoliaRec.Header().Get("X-Ebeacon-Cache"); got == "HIT" {
		t.Fatalf("first sepolia request unexpectedly served from cache")
	}

	secondSepolia := httptest.NewRequest(http.MethodGet, "/"+sepoliaID+"/eth/v1/node/version", nil)
	secondSepoliaRec := httptest.NewRecorder()
	p.ServeHTTP(secondSepoliaRec, secondSepolia)
	if secondSepoliaRec.Code != http.StatusOK {
		t.Fatalf("second sepolia request: status %d body %q", secondSepoliaRec.Code, secondSepoliaRec.Body.String())
	}
	if got := secondSepoliaRec.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("second sepolia cache header: got %q want HIT", got)
	}
	if !strings.Contains(secondSepoliaRec.Body.String(), "from-sepolia") {
		t.Fatalf("second sepolia body: %q", secondSepoliaRec.Body.String())
	}

	if got := mainnetHits.Load(); got != 1 {
		t.Fatalf("mainnet upstream hits: got %d want 1", got)
	}
	if got := sepoliaHits.Load(); got != 1 {
		t.Fatalf("sepolia upstream hits: got %d want 1", got)
	}
}

func TestProxy_UnknownNetwork(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	// Two networks so the proxy is not in single-network prefix-free mode.
	idA, idB := uniquePair(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	yaml := fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams: [{ id: u1, url: %q }]
    routing: { loadBalancing: round-robin, stickySession: false }
    cache: { enabled: false }
  - id: %s
    upstreams: [{ id: u2, url: %q }]
    routing: { loadBalancing: round-robin, stickySession: false }
    cache: { enabled: false }
`, idA, up.URL, idB, up.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	na, err := networkpkg.New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	nb, err := networkpkg.New(&cfg.Networks[1], cfg)
	if err != nil {
		t.Fatal(err)
	}

	p := New(map[string]*networkpkg.Network{idA: na, idB: nb})

	req := httptest.NewRequest(http.MethodGet, "/unknown/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unknown network") {
		t.Fatalf("body: %q", rec.Body.String())
	}
}

func TestProxy_PathEmbeddedKeyAndClientRoute(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v1/node/version" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":"client-route"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer up.Close()

	netID := networkID(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	yaml := fmt.Sprintf(`logLevel: error
server:
  host: "127.0.0.1"
  port: 5555
  maxTimeout: 30s
failsafe:
  timeout:
    duration: 10s
health:
  checkInterval: 1h
  finalityInterval: 1h
  maxSyncDistance: 10
rateLimiting: {}
metrics:
  enabled: false
networks:
  - id: %s
    upstreams:
      - id: lighthouse
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/lighthouse/"
          upstreamId: lighthouse
    cache:
      enabled: false
`, netID, up.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := networkpkg.New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	p := New(map[string]*networkpkg.Network{netID: n})
	p.SetAuth(&config.AuthConfig{
		Keys: []config.APIKeyConfig{{ID: "path-key", Secret: "path-secret-xyz"}},
	})

	req := httptest.NewRequest(http.MethodGet, "/path-secret-xyz/"+netID+"/lighthouse/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("path auth + client route: status %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "client-route") {
		t.Fatalf("body: %q", rec.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodGet, "/"+netID+"/eth/v1/node/version", nil)
	req2.Header.Set(headerAPIKey, "path-secret-xyz")
	rec2 := httptest.NewRecorder()
	p.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("header auth on standard path: status %d: %s", rec2.Code, rec2.Body.String())
	}

	req3 := httptest.NewRequest(http.MethodGet, "/wrong-secret/"+netID+"/eth/v1/node/version", nil)
	rec3 := httptest.NewRecorder()
	p.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusUnauthorized {
		t.Fatalf("wrong path key: expected 401, got %d", rec3.Code)
	}
}

func TestProxy_NetworkThenPathKey(t *testing.T) {
	t.Parallel()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v1/node/version" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":"ok"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer up.Close()

	netID := networkID(t)
	cfg := mustLoadConfig(t, netID, up.URL)
	n, err := networkpkg.New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	p := New(map[string]*networkpkg.Network{netID: n})
	p.SetAuth(&config.AuthConfig{
		Keys: []config.APIKeyConfig{{ID: "k", Secret: "secret-after-net"}},
	})

	// /{network}/{api-key}/... should authenticate and route correctly.
	req := httptest.NewRequest(http.MethodGet, "/"+netID+"/secret-after-net/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("network-then-key: status %d: %s", rec.Code, rec.Body.String())
	}

	// Wrong key in that position should be 401.
	req2 := httptest.NewRequest(http.MethodGet, "/"+netID+"/wrong-secret/eth/v1/node/version", nil)
	rec2 := httptest.NewRecorder()
	p.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key after network: expected 401, got %d", rec2.Code)
	}

	// /{network}/eth/... without a path key should still work via header.
	req3 := httptest.NewRequest(http.MethodGet, "/"+netID+"/eth/v1/node/version", nil)
	req3.Header.Set(headerAPIKey, "secret-after-net")
	rec3 := httptest.NewRecorder()
	p.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("header auth on standard path: status %d: %s", rec3.Code, rec3.Body.String())
	}
}

func TestProxy_TierRateLimit(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v1/node/version" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":"ok"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer up.Close()

	netID := networkID(t)
	cfg := mustLoadConfig(t, netID, up.URL)
	n, err := networkpkg.New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	p := New(map[string]*networkpkg.Network{netID: n})
	p.SetAuth(&config.AuthConfig{
		Keys: []config.APIKeyConfig{
			{ID: "free-user", Secret: "free-secret", Tier: "free"},
		},
		Tiers: []config.TierConfig{
			{
				Name: "free",
				RateLimiting: &config.RateLimitConfig{
					Limit: 1,
					Burst: 1,
				},
			},
		},
	})

	path := "/" + netID + "/eth/v1/node/version"
	req1 := httptest.NewRequest(http.MethodGet, path, nil)
	req1.Header.Set(headerAPIKey, "free-secret")
	rec1 := httptest.NewRecorder()
	p.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, path, nil)
	req2.Header.Set(headerAPIKey, "free-secret")
	rec2 := httptest.NewRecorder()
	p.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: expected 429, got %d", rec2.Code)
	}
	if got := rec2.Header().Get("X-Ebeacon-Rate-Limited"); got != "tier:free" {
		t.Fatalf("rate limit header: got %q", got)
	}
}

func TestProxy_Healthz(t *testing.T) {
	idA, idB := uniquePair(t)
	cfgA := mustLoadConfig(t, idA, "http://127.0.0.1:1")
	nA, err := networkpkg.New(&cfgA.Networks[0], cfgA)
	if err != nil {
		t.Fatal(err)
	}
	cfgB := mustLoadConfig(t, idB, "http://127.0.0.1:2")
	nB, err := networkpkg.New(&cfgB.Networks[0], cfgB)
	if err != nil {
		t.Fatal(err)
	}
	nA.Pool().All()[0].SetHealth(upstream.HealthDegraded)
	nB.Pool().All()[0].SetHealth(upstream.HealthUp)

	p := New(map[string]*networkpkg.Network{idB: nB, idA: nA})
	p.SetAuth(&config.AuthConfig{
		Keys: []config.APIKeyConfig{{ID: "key", Secret: "secret"}},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("degraded healthz: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type: got %q", got)
	}
	expected := fmt.Sprintf(`{"status":"degraded","networks":{%q:"degraded",%q:"ok"}}`, idA, idB)
	if got := rec.Body.String(); got != expected {
		t.Fatalf("body: got %q want %q", got, expected)
	}

	nA.Pool().All()[0].SetHealth(upstream.HealthDown)
	nB.Pool().All()[0].SetHealth(upstream.HealthDown)

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	p.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("down healthz: got %d body %q", rec2.Code, rec2.Body.String())
	}
	expected2 := fmt.Sprintf(`{"status":"down","networks":{%q:"down",%q:"down"}}`, idA, idB)
	if got := rec2.Body.String(); got != expected2 {
		t.Fatalf("body: got %q want %q", got, expected2)
	}
}
