package network

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ebeacon/ebeacon/config"
)

func TestNetwork_ClientRoutePinsUpstream(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v1/node/version" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"version":"from-a"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"version":"from-b"}}`))
	}))
	defer b.Close()

	id := netID(t)
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
    upstreams:
      - id: a
        url: %q
      - id: b
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/lighthouse/"
          upstreamId: a
    cache:
      enabled: false
`, id, a.URL, b.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/lighthouse/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "from-a") {
		t.Fatalf("body %q", rec.Body.String())
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("a") {
		t.Fatalf("upstream header: %q", got)
	}
}

func TestNetwork_ClientRouteUnavailableReturns503(t *testing.T) {
	var backupHits int
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupHits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"version":"from-backup"}}`))
	}))
	defer backup.Close()

	id := netID(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	yaml := fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe:
  timeout: { duration: 10s }
  retry: { maxAttempts: 2, delay: 1ms, backoff: 1, maxDelay: 1ms }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: a
        url: "http://127.0.0.1:1"
      - id: b
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/a/"
          upstreamId: a
    cache:
      enabled: false
`, id, backup.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/a/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if backupHits != 0 {
		t.Fatalf("backup hits: got %d want 0", backupHits)
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != "" {
		t.Fatalf("unexpected upstream header: %q", got)
	}
	if !strings.Contains(rec.Body.String(), "selected upstream unavailable") {
		t.Fatalf("body %q", rec.Body.String())
	}
}

func TestNetwork_ClientRoute5xxDoesNotFallback(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"selected"}`))
	}))
	defer a.Close()

	var backupHits int
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupHits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"backup"}`))
	}))
	defer b.Close()

	id := netID(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	yaml := fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe:
  timeout: { duration: 10s }
  retry: { maxAttempts: 2, delay: 1ms, backoff: 1, maxDelay: 1ms }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: a
        url: %q
      - id: b
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/a/"
          upstreamId: a
    cache:
      enabled: false
`, id, a.URL, b.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/a/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if backupHits != 0 {
		t.Fatalf("backup hits: got %d want 0", backupHits)
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("a") {
		t.Fatalf("upstream header: got %q want %q", got, ObfuscateUpstreamID("a"))
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"selected"}` {
		t.Fatalf("body: got %q", got)
	}
}

func TestNetwork_RouteRuleDeny(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)
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
    upstreams:
      - id: u1
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      routeRules:
        - pathPattern: "^/eth/v1/debug/.*"
          deny: true
    cache:
      enabled: false
`, id, up.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/debug/foo", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestNetwork_RouteRuleUpstreamOverride(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`a`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`b`))
	}))
	defer b.Close()

	id := netID(t)
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
    upstreams:
      - id: a
        url: %q
      - id: b
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      routeRules:
        - pathPattern: "^/eth/v1/node/version$"
          methods: [GET]
          upstreamId: b
    cache:
      enabled: false
`, id, a.URL, b.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %q", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "b" {
		t.Fatalf("body %q want b", rec.Body.String())
	}
}

func TestNetwork_ClientRouteCacheScopedPerPinnedUpstream(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`from-a`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`from-b`))
	}))
	defer b.Close()

	id := netID(t)
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
    upstreams:
      - id: a
        url: %q
      - id: b
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/a/"
          upstreamId: a
        - pathPrefix: "/b/"
          upstreamId: b
    cache:
      enabled: true
`, id, a.URL, b.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	recDefault := httptest.NewRecorder()
	n.ServeHTTP(recDefault, httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil))
	if recDefault.Code != http.StatusOK {
		t.Fatalf("default status %d %q", recDefault.Code, recDefault.Body.String())
	}

	recPinned := httptest.NewRecorder()
	n.ServeHTTP(recPinned, httptest.NewRequest(http.MethodGet, "/b/eth/v1/node/version", nil))
	if recPinned.Code != http.StatusOK {
		t.Fatalf("pinned status %d %q", recPinned.Code, recPinned.Body.String())
	}
	if recPinned.Body.String() != "from-b" {
		t.Fatalf("body %q want from-b", recPinned.Body.String())
	}
	if got := recPinned.Header().Get("X-Ebeacon-Cache"); got == "HIT" {
		t.Fatal("client-pinned route should not reuse the shared cache entry")
	}
}
