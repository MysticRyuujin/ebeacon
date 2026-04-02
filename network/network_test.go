package network

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ebeacon/ebeacon/config"
	"github.com/ebeacon/ebeacon/debuglog"
	"github.com/ebeacon/ebeacon/upstream"
)

var netLabelSeq atomic.Uint64

func netID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("net_%d_%s", netLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_"))
}

func mustCfg(t *testing.T, id, upstream string, blocked []string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	blockedYAML := "[]"
	if len(blocked) > 0 {
		var b strings.Builder
		b.WriteString("[\n")
		for i, p := range blocked {
			if i > 0 {
				b.WriteString(",\n")
			}
			b.WriteString(`        "` + strings.ReplaceAll(p, `"`, `\"`) + `"`)
		}
		b.WriteString("\n      ]")
		blockedYAML = b.String()
	}
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
      blockedPaths: %s
    cache:
      enabled: false
`, id, upstream, blockedYAML)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

func mustCfgText(t *testing.T, yaml string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

func TestNetwork_BlockedPath_403(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, []string{`^/eth/v1/debug/.*`})

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/debug/something", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
}

func TestNetwork_Forward_OK(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/node/version" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"version":"ok"}}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("u1") {
		t.Fatalf("X-Ebeacon-Upstream: got %q", got)
	}
}

func TestShouldTreatStatusAsPathError(t *testing.T) {
	tests := []struct {
		name       string
		apiPath    string
		statusCode int
		want       bool
	}{
		{name: "rewards 404", apiPath: "/eth/v1/beacon/rewards/attestations/{epoch}", statusCode: http.StatusNotFound, want: true},
		{name: "blob sidecars 405", apiPath: "/eth/v1/beacon/blob_sidecars/{block_id}", statusCode: http.StatusMethodNotAllowed, want: true},
		{name: "regular endpoint 404", apiPath: "/eth/v1/node/version", statusCode: http.StatusNotFound, want: false},
		{name: "rewards 500", apiPath: "/eth/v1/beacon/rewards/blocks/{block_id}", statusCode: http.StatusInternalServerError, want: false},
	}

	for _, tt := range tests {
		if got := shouldTreatStatusAsPathError(tt.apiPath, tt.statusCode); got != tt.want {
			t.Fatalf("%s: got %t want %t", tt.name, got, tt.want)
		}
	}
}

func TestNetwork_DebugLogging_LogsSwallowedUpstream500(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"upstream boom"}`))
	}))
	defer up.Close()

	logPath := filepath.Join(t.TempDir(), "debug.log")
	logger, closer, err := debuglog.New(config.DebugLoggingConfig{
		Enabled:      true,
		Path:         logPath,
		MaxSizeMB:    1,
		MaxBackups:   1,
		MaxBodyBytes: 4096,
	})
	if err != nil {
		t.Fatalf("debuglog.New: %v", err)
	}
	defer closer.Close() //nolint:errcheck
	prev := debuglog.SetDefault(logger)
	defer debuglog.SetDefault(prev)

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version?secret=shh", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	logged := string(data)
	if !strings.Contains(logged, `"kind":"upstream_attempt_failed"`) {
		t.Fatalf("expected upstream_attempt_failed log entry, got %s", logged)
	}
	if !strings.Contains(logged, `upstream boom`) {
		t.Fatalf("expected upstream response body in log, got %s", logged)
	}
	if strings.Contains(logged, `secret-token`) {
		t.Fatalf("authorization header was not redacted: %s", logged)
	}
	if strings.Contains(logged, `secret=shh`) {
		t.Fatalf("query secret was not redacted: %s", logged)
	}
}

func TestNetwork_EffectiveFailsafe_ValidatorOverrideMatchesCollectionAndItem(t *testing.T) {
	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 300s }
failsafe:
  timeout: { duration: 30s }
  retry: { maxAttempts: 3, delay: 100ms, backoff: 2.0, jitter: 50ms, maxDelay: 5s }
  hedge: { delay: 500ms, maxCount: 1 }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: u1
        url: "http://127.0.0.1:5052"
    failsafeOverrides:
      - pathPattern: "^/eth/v1/beacon/states/[^/]+/validators(?:/[^/]+)?$"
        methods: [GET]
        failsafe:
          timeout: { duration: 300s }
          retry: { maxAttempts: 2 }
          hedge: { maxCount: 0 }
    routing:
      loadBalancing: round-robin
      stickySession: false
    cache:
      enabled: false
`, id))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{
		"/eth/v1/beacon/states/head/validators",
		"/eth/v1/beacon/states/14027359/validators/1",
	} {
		fs := n.effectiveFailsafe(http.MethodGet, path)
		if fs.Timeout == nil || fs.Timeout.Duration != 300*time.Second {
			t.Fatalf("timeout for %s: got %#v", path, fs.Timeout)
		}
		if fs.Retry == nil || fs.Retry.MaxAttempts != 2 {
			t.Fatalf("retry for %s: got %#v", path, fs.Retry)
		}
		if fs.Hedge == nil || fs.Hedge.MaxCount != 0 {
			t.Fatalf("hedge for %s: got %#v", path, fs.Hedge)
		}
	}
}

func TestNetwork_NodeHealth_ClientScoped(t *testing.T) {
	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: lh-1
        url: "http://127.0.0.1:1"
      - id: teku-1
        url: "http://127.0.0.1:2"
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/lighthouse/"
          upstreamId: client:lighthouse
    cache:
      enabled: false
`, id))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range n.Pool().All() {
		switch u.ID {
		case "lh-1":
			u.SetClientType(upstream.ClientLighthouse)
			u.SetHealth(upstream.HealthDegraded)
		case "teku-1":
			u.SetClientType(upstream.ClientTeku)
			u.SetHealth(upstream.HealthUp)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/lighthouse/eth/v1/node/health", nil)
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("scoped health: got %d body %q", rec.Code, rec.Body.String())
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/eth/v1/node/health", nil)
	n.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("global health: got %d body %q", rec2.Code, rec2.Body.String())
	}
}

func TestNetwork_Healthz(t *testing.T) {
	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: a
        url: "http://127.0.0.1:1"
      - id: b
        url: "http://127.0.0.1:2"
    routing:
      loadBalancing: round-robin
      stickySession: false
    cache:
      enabled: false
`, id))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range n.Pool().All() {
		switch u.ID {
		case "a":
			u.SetHealth(upstream.HealthDegraded)
		case "b":
			u.SetHealth(upstream.HealthDown)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("degraded healthz: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type: got %q", got)
	}
	if got := rec.Body.String(); got != `{"status":"degraded","upstreams":2,"healthy":0,"degraded":1,"down":1}` {
		t.Fatalf("body: got %q", got)
	}

	for _, u := range n.Pool().All() {
		u.SetHealth(upstream.HealthDown)
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	n.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("down healthz: got %d body %q", rec2.Code, rec2.Body.String())
	}
	if got := rec2.Body.String(); got != `{"status":"down","upstreams":2,"healthy":0,"degraded":0,"down":2}` {
		t.Fatalf("body: got %q", got)
	}
}

func TestNetwork_Healthz_ClientScoped(t *testing.T) {
	// Two upstreams: one teku (healthy), one lighthouse (down).
	// /teku/healthz should report only the teku upstream.
	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: teku-1
        url: "http://127.0.0.1:1"
      - id: lighthouse-1
        url: "http://127.0.0.1:2"
    routing:
      loadBalancing: round-robin
      stickySession: false
    cache:
      enabled: false
`, id))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range n.Pool().All() {
		switch u.ID {
		case "teku-1":
			u.SetClientType(upstream.ClientTeku)
			u.SetHealth(upstream.HealthUp)
		case "lighthouse-1":
			u.SetClientType(upstream.ClientLighthouse)
			u.SetHealth(upstream.HealthDown)
		}
	}

	// /teku/healthz → scoped to teku upstream (healthy)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/teku/healthz", nil)
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("teku healthz: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != `{"status":"ok","upstreams":1,"healthy":1,"degraded":0,"down":0}` {
		t.Fatalf("teku body: got %q", got)
	}

	// /lighthouse/healthz → scoped to lighthouse upstream (down)
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/lighthouse/healthz", nil)
	n.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("lighthouse healthz: got %d body %q", rec2.Code, rec2.Body.String())
	}
	if got := rec2.Body.String(); got != `{"status":"down","upstreams":1,"healthy":0,"degraded":0,"down":1}` {
		t.Fatalf("lighthouse body: got %q", got)
	}
}

func TestNetwork_Healthz_ClientRoute(t *testing.T) {
	// Client route configured via clientRoutes: /a/ → upstream "a".
	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    upstreams:
      - id: a
        url: "http://127.0.0.1:1"
      - id: b
        url: "http://127.0.0.1:2"
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/a/"
          upstreamId: a
    cache:
      enabled: false
`, id))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range n.Pool().All() {
		switch u.ID {
		case "a":
			u.SetHealth(upstream.HealthDegraded)
		case "b":
			u.SetHealth(upstream.HealthUp)
		}
	}

	// /a/healthz → scoped to upstream "a" (degraded)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/a/healthz", nil)
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("a healthz: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != `{"status":"degraded","upstreams":1,"healthy":0,"degraded":1,"down":0}` {
		t.Fatalf("a body: got %q", got)
	}
}

func TestNetwork_AllUpstreamsFail_502(t *testing.T) {
	// Nothing listens on this port in practice for the test host.
	badURL := "http://127.0.0.1:1"
	id := netID(t)
	cfg := mustCfg(t, id, badURL, nil)

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status: got %d %q", rec.Code, rec.Body.String())
	}
}

func TestEffectiveCacheTTL_FinalizedSlot(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	n.pool.UpdateFinalizedEpoch(100)

	// Path references slot 50 — finalized (50 <= 100*32+31).
	ttl := n.effectiveCacheTTL(time.Minute, "/eth/v2/beacon/blocks/50")
	if ttl != 0 {
		t.Fatalf("expected forever (0) for finalized slot, got %v", ttl)
	}

	ttl2 := n.effectiveCacheTTL(time.Minute, "/eth/v2/beacon/blocks/999999999")
	if ttl2 != time.Minute {
		t.Fatalf("expected policy TTL for non-finalized slot, got %v", ttl2)
	}
}

func TestEffectiveCacheTTL_FinalizedEpoch(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	n.pool.UpdateFinalizedEpoch(100)

	ttl := n.effectiveCacheTTL(30*time.Second, "/eth/v1/beacon/rewards/attestations/99")
	if ttl != 0 {
		t.Fatalf("expected forever (0) for finalized epoch rewards, got %v", ttl)
	}

	ttl2 := n.effectiveCacheTTL(30*time.Second, "/eth/v1/validator/duties/proposer/101")
	if ttl2 != 30*time.Second {
		t.Fatalf("expected policy TTL for non-finalized epoch, got %v", ttl2)
	}
}

func TestNetwork_StartStop(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v1/node/syncing":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"head_slot":"1","sync_distance":"0","is_syncing":false}}`))
		case "/eth/v1/beacon/states/head/finality_checkpoints":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"finalized":{"epoch":"0"}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	// Short intervals so health loop runs quickly; cancel immediately after start.
	cfg.Health.CheckInterval = 50 * time.Millisecond
	cfg.Health.FinalityInterval = 50 * time.Millisecond

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.Start(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestCopyHeaders_DoNotMutateSource(t *testing.T) {
	srcReq := http.Header{
		"Connection":             []string{"keep-alive"},
		"Authorization":          []string{"Bearer local-token"},
		"X-Api-Key":              []string{"premium-local-test-key"},
		"X-Ebeacon-Secret-Token": []string{"local-secret"},
		"X-Ebeacon-Test":         []string{"ok"},
		"X-Ebeacon-Use-Upstream": []string{"u1"},
	}
	dstReq := http.Header{}
	copyRequestHeaders(dstReq, srcReq)
	if got := dstReq.Get("Connection"); got != "" {
		t.Fatalf("request hop-by-hop header copied: %q", got)
	}
	if got := dstReq.Get("X-Ebeacon-Test"); got != "ok" {
		t.Fatalf("request header missing: %q", got)
	}
	if got := dstReq.Get("Authorization"); got != "" {
		t.Fatalf("request auth header copied upstream: %q", got)
	}
	if got := dstReq.Get("X-API-Key"); got != "" {
		t.Fatalf("request API key header copied upstream: %q", got)
	}
	if got := dstReq.Get("X-EBEACON-Secret-Token"); got != "" {
		t.Fatalf("request secret header copied upstream: %q", got)
	}
	if got := srcReq.Get("Connection"); got != "keep-alive" {
		t.Fatalf("source request headers mutated: %q", got)
	}
	if got := srcReq.Get("Authorization"); got != "Bearer local-token" {
		t.Fatalf("source request auth header mutated: %q", got)
	}
	if got := srcReq.Get("X-API-Key"); got != "premium-local-test-key" {
		t.Fatalf("source request api key header mutated: %q", got)
	}
	if got := srcReq.Get("X-EBEACON-Secret-Token"); got != "local-secret" {
		t.Fatalf("source request secret header mutated: %q", got)
	}
	if got := srcReq.Get("X-Ebeacon-Use-Upstream"); got != "u1" {
		t.Fatalf("source request upstream directive mutated: %q", got)
	}

	srcResp := http.Header{
		"Transfer-Encoding": []string{"chunked"},
		"Content-Type":      []string{"application/json"},
	}
	dstResp := http.Header{}
	copyResponseHeaders(dstResp, srcResp)
	if got := dstResp.Get("Transfer-Encoding"); got != "" {
		t.Fatalf("response hop-by-hop header copied: %q", got)
	}
	if got := dstResp.Get("Content-Type"); got != "application/json" {
		t.Fatalf("response header missing: %q", got)
	}
	if got := srcResp.Get("Transfer-Encoding"); got != "chunked" {
		t.Fatalf("source response headers mutated: %q", got)
	}
}

func TestForward_TracksActiveConnectionsUntilBodyClose(t *testing.T) {
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"ok"}`))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	u := n.pool.ByID("u1")
	if u == nil {
		t.Fatal("expected upstream u1")
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	resp, err := n.forward(context.Background(), u, req, nil)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if got := u.ActiveConns(); got != 1 {
		t.Fatalf("active connections before close: got %d want 1", got)
	}

	close(release)
	_, _ = io.ReadAll(resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	if got := u.ActiveConns(); got != 0 {
		t.Fatalf("active connections after close: got %d want 0", got)
	}
}

func TestNetwork_JSONResponsesReturnedAsIs(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"version":"ok"}}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Networks[0].Cache.Enabled = true

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req1 := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec1 := httptest.NewRecorder()
	n.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first status: got %d body %q", rec1.Code, rec1.Body.String())
	}
	if got := rec1.Body.String(); got != `{"data":{"version":"ok"}}` {
		t.Fatalf("first body: got %q", got)
	}
	wantLen := fmt.Sprintf("%d", len([]byte(`{"data":{"version":"ok"}}`)))
	if got := rec1.Header().Get("Content-Length"); got != wantLen {
		t.Fatalf("first content-length: got %q want %q", got, wantLen)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second status: got %d body %q", rec2.Code, rec2.Body.String())
	}
	if got := rec2.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("second request should be cache hit, got %q", got)
	}
	if got := rec2.Body.String(); got != `{"data":{"version":"ok"}}` {
		t.Fatalf("cached body: got %q", got)
	}
	if hits.Load() != 1 {
		t.Fatalf("upstream hits: got %d want 1", hits.Load())
	}
}

func TestNetwork_TruncatedUpstreamBodyReturns502(t *testing.T) {
	id := netID(t)
	cfg := mustCfg(t, id, "http://127.0.0.1:1", nil)

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	u := n.pool.ByID("u1")
	if u == nil {
		t.Fatal("expected upstream u1")
	}
	u.Client = &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			ContentLength: 32,
			Body: &partialReadCloser{
				payload: []byte(`{"data":{"version":"truncated`),
				err:     io.ErrUnexpectedEOF,
			},
		}, nil
	})}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `{"data":{"version":"truncated`) {
		t.Fatalf("partial upstream body leaked to client: %q", rec.Body.String())
	}
	if got := rec.Header().Get("X-Ebeacon-Cache"); got != "" {
		t.Fatalf("unexpected cache header on truncated upstream response: %q", got)
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != "" {
		t.Fatalf("unexpected upstream header on truncated upstream response: %q", got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != "bad gateway" {
		t.Fatalf("body: got %q want %q", body, "bad gateway")
	}
	if got := u.ActiveConns(); got != 0 {
		t.Fatalf("active connections after truncated read: got %d want 0", got)
	}
}

func TestNetwork_HedgePreservesWinningResponseBody(t *testing.T) {
	payload := `{"data":"` + strings.Repeat("x", 4096) + `"}`
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(payload[:32])); err != nil {
			return
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(payload[32:]))
	}))
	defer primary.Close()

	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(250 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":"secondary"}`))
	}))
	defer secondary.Close()

	id := netID(t)
	cfg := mustCfg(t, id, primary.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "u1", URL: primary.URL},
		{ID: "u2", URL: secondary.URL},
	}
	cfg.Failsafe.Hedge = &config.HedgeConfig{Delay: 10 * time.Millisecond, MaxCount: 1}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != payload {
		t.Fatalf("body length: got %d want %d", len(got), len(payload))
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("u1") {
		t.Fatalf("X-Ebeacon-Upstream: got %q want %q", got, ObfuscateUpstreamID("u1"))
	}
}

func TestNetwork_OctetStreamResponsesStayByteExact(t *testing.T) {
	want := []byte{0x00, 0x01, 0x02, 0x03, 0xff}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(want)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Networks[0].Cache.Enabled = true
	cfg.Networks[0].Cache.Policies = []config.CachePolicy{
		{
			Pattern: `^/eth/v1/beacon/blobs/\d+$`,
			TTL:     time.Minute,
		},
	}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req1 := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/123", nil)
	rec1 := httptest.NewRecorder()
	n.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first status: got %d", rec1.Code)
	}
	if !bytes.Equal(rec1.Body.Bytes(), want) {
		t.Fatalf("first body changed: got %v want %v", rec1.Body.Bytes(), want)
	}
	if got := rec1.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("content-type: got %q", got)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/123", nil)
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, req2)
	if got := rec2.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("expected cache hit for octet-stream, got %q", got)
	}
	if !bytes.Equal(rec2.Body.Bytes(), want) {
		t.Fatalf("cached body changed: got %v want %v", rec2.Body.Bytes(), want)
	}
}

func TestNetwork_GlobalRateLimit_Header(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`ok`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.RateLimiting.Global = &config.RateLimitConfig{Limit: 1, Burst: 1}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	rec1 := httptest.NewRecorder()
	n.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil))
	if rec1.Code != http.StatusOK {
		t.Fatalf("first status: got %d", rec1.Code)
	}

	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil))
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second status: got %d", rec2.Code)
	}
	if got := rec2.Header().Get("X-Ebeacon-Rate-Limited"); got != "global" {
		t.Fatalf("rate-limited header: got %q", got)
	}
}

func TestNetwork_ClientRouteByDetectedClientType(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"version":"from-a"}}`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
        - pathPrefix: "/teku/"
          upstreamId: "client:teku"
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
	n.Pool().ByID("b").SetClientType("teku")

	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/teku/eth/v1/node/version", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "from-b") {
		t.Fatalf("expected client:teku route to upstream b, body %q", rec.Body.String())
	}
}

func TestNetwork_AutoDetectedPathSelectorByClientType(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"version":"from-a"}}`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	n.Pool().ByID("b").SetClientType("teku")

	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/teku/eth/v1/node/version", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "from-b") {
		t.Fatalf("expected auto-detected teku selector to reach upstream b, body %q", rec.Body.String())
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("b") {
		t.Fatalf("upstream header: got %q want %q", got, ObfuscateUpstreamID("b"))
	}
}

func TestNetwork_MultiplexDeduplicatesConcurrentGET(t *testing.T) {
	var upstreamHits atomic.Int32
	start := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		<-start
		time.Sleep(20 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"version":"ok"}}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	const parallel = 8
	var wg sync.WaitGroup
	wg.Add(parallel)
	errs := make(chan error, parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
			n.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				errs <- fmt.Errorf("status %d", rec.Code)
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if hits := upstreamHits.Load(); hits != 1 {
		t.Fatalf("expected one upstream call via multiplexing, got %d", hits)
	}
}

func TestNetwork_PreservesEthHeadersAndClientType(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Eth-Consensus-Version", "deneb")
		_, _ = w.Write([]byte(`{"data":{"version":"ok"}}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	n.Pool().ByID("u1").SetClientType("lighthouse")

	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil))
	if got := rec.Header().Get("Eth-Consensus-Version"); got != "deneb" {
		t.Fatalf("Eth-Consensus-Version not preserved, got %q", got)
	}
	if got := rec.Header().Get("X-Ebeacon-Client-Type"); got != "lighthouse" {
		t.Fatalf("X-Ebeacon-Client-Type: got %q", got)
	}
}

func TestNetwork_GzipDecompressAndRecompress(t *testing.T) {
	payload := `{"data":"` + strings.Repeat("x", 2200) + `"}`
	var gzBuf bytes.Buffer
	gzWriter := gzip.NewWriter(&gzBuf)
	if _, err := gzWriter.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	if err := gzWriter.Close(); err != nil {
		t.Fatal(err)
	}

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(gzBuf.Bytes())
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip response, got %q", got)
	}

	gr, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("read gzip response: %v", err)
	}
	out, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("decompress response: %v", err)
	}
	if err := gr.Close(); err != nil {
		t.Fatalf("close gzip response: %v", err)
	}
	if !strings.Contains(string(out), strings.Repeat("x", 2000)) {
		t.Fatalf("unexpected decompressed payload length=%d", len(out))
	}
}

func TestNetwork_GzipUpstreamPlainClientReturnsPlainBody(t *testing.T) {
	payload := `{"data":"` + strings.Repeat("x", 2200) + `"}`
	var gzBuf bytes.Buffer
	gzWriter := gzip.NewWriter(&gzBuf)
	if _, err := gzWriter.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	if err := gzWriter.Close(); err != nil {
		t.Fatal(err)
	}

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(gzBuf.Bytes())
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("expected plain response, got content-encoding %q", got)
	}
	if got := rec.Body.String(); got != payload {
		t.Fatalf("body mismatch: got %d want %d", len(got), len(payload))
	}
	if got := rec.Header().Get("Content-Length"); got != fmt.Sprintf("%d", len(payload)) {
		t.Fatalf("content-length: got %q want %d", got, len(payload))
	}
}

func TestNetwork_PlainLargeResponseCompressedForGzipClient(t *testing.T) {
	payload := `{"data":"` + strings.Repeat("x", 2200) + `"}`
	var acceptEncoding string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acceptEncoding = r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	if acceptEncoding != "gzip" {
		t.Fatalf("upstream accept-encoding: got %q want %q", acceptEncoding, "gzip")
	}
	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip response, got %q", got)
	}
	if got := gunzipString(t, rec.Body.Bytes()); got != payload {
		t.Fatalf("gunzip body mismatch: got %d want %d", len(got), len(payload))
	}
}

func TestNetwork_SmallPlainResponseNotCompressedForGzipClient(t *testing.T) {
	payload := `{"data":"small"}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("small response should stay plain, got %q", got)
	}
	if got := rec.Body.String(); got != payload {
		t.Fatalf("body: got %q want %q", got, payload)
	}
}

func TestNetwork_CachedLargeResponseCompressedForGzipClient(t *testing.T) {
	var hits atomic.Int32
	payload := `{"data":"` + strings.Repeat("x", 2200) + `"}`
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Networks[0].Cache.Enabled = true
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req1 := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec1 := httptest.NewRecorder()
	n.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first status: got %d", rec1.Code)
	}
	if got := rec1.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("first response should stay plain, got %q", got)
	}
	if got := rec1.Body.String(); got != payload {
		t.Fatalf("first body mismatch: got %d want %d", len(got), len(payload))
	}

	req2 := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req2.Header.Set("Accept-Encoding", "gzip")
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second status: got %d", rec2.Code)
	}
	if got := rec2.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("second request should be cache hit, got %q", got)
	}
	if got := rec2.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("cached large response should be recompressed, got %q", got)
	}
	if got := gunzipString(t, rec2.Body.Bytes()); got != payload {
		t.Fatalf("cached gunzip body mismatch: got %d want %d", len(got), len(payload))
	}
	if hits.Load() != 1 {
		t.Fatalf("expected one upstream hit, got %d", hits.Load())
	}
}

func TestNetwork_CachedLargeOctetStreamCompressedForGzipClient(t *testing.T) {
	var hits atomic.Int32
	want := bytes.Repeat([]byte{0xab, 0xcd, 0xef, 0x42}, 600)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(want)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Networks[0].Cache.Enabled = true
	cfg.Networks[0].Cache.Policies = []config.CachePolicy{{
		Pattern: `^/eth/v1/beacon/blobs/\d+$`,
		TTL:     time.Minute,
	}}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req1 := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/123", nil)
	req1.Header.Set("Accept", "application/octet-stream")
	rec1 := httptest.NewRecorder()
	n.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first status: got %d", rec1.Code)
	}
	if !bytes.Equal(rec1.Body.Bytes(), want) {
		t.Fatalf("first body changed: got %d want %d", len(rec1.Body.Bytes()), len(want))
	}

	req2 := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/123", nil)
	req2.Header.Set("Accept", "application/octet-stream")
	req2.Header.Set("Accept-Encoding", "gzip")
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second status: got %d", rec2.Code)
	}
	if got := rec2.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("second request should be cache hit, got %q", got)
	}
	if got := rec2.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("content-type: got %q", got)
	}
	if got := rec2.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip response, got %q", got)
	}
	if got := gunzipBytes(t, rec2.Body.Bytes()); !bytes.Equal(got, want) {
		t.Fatalf("cached binary mismatch after gunzip: got %d want %d", len(got), len(want))
	}
	if hits.Load() != 1 {
		t.Fatalf("expected one upstream hit, got %d", hits.Load())
	}
}

func TestNetwork_JSONAndOctetStreamCacheStaySeparate(t *testing.T) {
	var jsonHits atomic.Int32
	var binaryHits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Accept"), "application/octet-stream") {
			binaryHits.Add(1)
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte{0x01, 0x02, 0x03, 0x04})
			return
		}
		jsonHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":"json"}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Networks[0].Cache.Enabled = true
	cfg.Networks[0].Cache.Policies = []config.CachePolicy{{
		Pattern: `^/eth/v1/beacon/blobs/\d+$`,
		TTL:     time.Minute,
	}}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	binaryReq := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/456", nil)
	binaryReq.Header.Set("Accept", "application/octet-stream")
	binaryRec := httptest.NewRecorder()
	n.ServeHTTP(binaryRec, binaryReq)
	if binaryRec.Code != http.StatusOK {
		t.Fatalf("binary status: got %d", binaryRec.Code)
	}

	jsonReq := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/456", nil)
	jsonRec := httptest.NewRecorder()
	n.ServeHTTP(jsonRec, jsonReq)
	if jsonRec.Code != http.StatusOK {
		t.Fatalf("json status: got %d", jsonRec.Code)
	}
	if got := jsonRec.Header().Get("X-Ebeacon-Cache"); got != "" {
		t.Fatalf("json response should not reuse octet-stream cache entry, got %q", got)
	}
	if got := jsonRec.Body.String(); got != `{"data":"json"}` {
		t.Fatalf("json body: got %q", got)
	}
	if binaryHits.Load() != 1 {
		t.Fatalf("binary hits: got %d want 1", binaryHits.Load())
	}
	if jsonHits.Load() != 1 {
		t.Fatalf("json hits: got %d want 1", jsonHits.Load())
	}
}

func TestBuildCacheKey_DropsNoisyNodeQueryParams(t *testing.T) {
	u := &url.URL{Path: "/eth/v1/node/peers", RawQuery: "fuzz=123&abc=456"}
	req := &http.Request{Method: http.MethodGet, URL: u}

	k1 := buildCacheKey("mainnet", req, "")
	u.RawQuery = "fuzz=999"
	k2 := buildCacheKey("mainnet", req, "")

	if k1 != k2 {
		t.Fatalf("expected equivalent cache keys for noisy peers query, got %q and %q", k1, k2)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type partialReadCloser struct {
	payload []byte
	err     error
	read    bool
}

func (p *partialReadCloser) Read(dst []byte) (int, error) {
	if p.read {
		return 0, io.EOF
	}
	p.read = true
	n := copy(dst, p.payload)
	return n, p.err
}

func (p *partialReadCloser) Close() error {
	return nil
}

func TestBuildCacheKey_KeepsQueryForOtherEndpoints(t *testing.T) {
	u := &url.URL{Path: "/eth/v1/beacon/states/head/validators", RawQuery: "status=active"}
	req := &http.Request{Method: http.MethodGet, URL: u}

	k1 := buildCacheKey("mainnet", req, "")
	u.RawQuery = "status=pending"
	k2 := buildCacheKey("mainnet", req, "")

	if k1 == k2 {
		t.Fatalf("expected different cache keys for meaningful query args, got %q", k1)
	}
}

func TestBuildCacheKey_IncludesSelectorScope(t *testing.T) {
	u := &url.URL{Path: "/eth/v1/node/version"}
	req := &http.Request{Method: http.MethodGet, URL: u}

	genericKey := buildCacheKey("mainnet", req, "")
	clientKey := buildCacheKey("mainnet", req, "client:nimbus")

	if genericKey == clientKey {
		t.Fatalf("expected selector-scoped cache key to differ, got %q", genericKey)
	}
}

func TestDedupKey_IncludesSelectorScope(t *testing.T) {
	u := &url.URL{Path: "/eth/v1/node/version"}

	genericKey := dedupKey("mainnet", http.MethodGet, u, nil, "")
	clientKey := dedupKey("mainnet", http.MethodGet, u, nil, "client:nimbus")

	if genericKey == clientKey {
		t.Fatalf("expected selector-scoped dedup key to differ, got %q", genericKey)
	}
}

func TestNetwork_POSTIsNotCached(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"version":"ok"}}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Networks[0].Cache.Enabled = true

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req1 := httptest.NewRequest(http.MethodPost, "/eth/v1/node/version", nil)
	rec1 := httptest.NewRecorder()
	n.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first status: got %d body %q", rec1.Code, rec1.Body.String())
	}
	if got := rec1.Header().Get("X-Ebeacon-Cache"); got != "" {
		t.Fatalf("first POST should not be cache hit, got %q", got)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/eth/v1/node/version", nil)
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second status: got %d body %q", rec2.Code, rec2.Body.String())
	}
	if got := rec2.Header().Get("X-Ebeacon-Cache"); got != "" {
		t.Fatalf("second POST should not be cache hit, got %q", got)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 upstream hits for uncached POST, got %d", hits.Load())
	}
}

func TestNetwork_HEADIsNotCached_CurrentBehavior(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Networks[0].Cache.Enabled = true

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req1 := httptest.NewRequest(http.MethodHead, "/eth/v1/node/version", nil)
	rec1 := httptest.NewRecorder()
	n.ServeHTTP(rec1, req1)
	req2 := httptest.NewRequest(http.MethodHead, "/eth/v1/node/version", nil)
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, req2)

	if got := rec2.Header().Get("X-Ebeacon-Cache"); got != "" {
		t.Fatalf("HEAD should not be cache hit under current behavior, got %q", got)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 upstream hits for uncached HEAD, got %d", hits.Load())
	}
}

func TestNetwork_Non2xxResponsesAreNotCached(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		http.NotFound(w, r)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Networks[0].Cache.Enabled = true

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req1 := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec1 := httptest.NewRecorder()
	n.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNotFound {
		t.Fatalf("first status: got %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("second status: got %d", rec2.Code)
	}
	if got := rec2.Header().Get("X-Ebeacon-Cache"); got != "" {
		t.Fatalf("non-2xx response should not be cache hit, got %q", got)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 upstream hits for uncached non-2xx, got %d", hits.Load())
	}
}

func TestNetwork_Upstream4xxReturnedWithoutRetry(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{name: "not_found", status: http.StatusNotFound},
		{name: "too_many_requests", status: http.StatusTooManyRequests},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var primaryHits atomic.Int32
			var backupHits atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				primaryHits.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(`{"error":"primary"}`))
			}))
			defer primary.Close()

			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				backupHits.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"data":"backup"}`))
			}))
			defer backup.Close()

			id := netID(t)
			cfg := mustCfg(t, id, primary.URL, nil)
			cfg.Networks[0].Upstreams = []config.UpstreamConfig{
				{ID: "u1", URL: primary.URL},
				{ID: "u2", URL: backup.URL},
			}
			cfg.Failsafe.Retry = &config.RetryConfig{MaxAttempts: 2, Delay: time.Millisecond, Backoff: 1, MaxDelay: time.Millisecond}

			n, err := New(&cfg.Networks[0], cfg)
			if err != nil {
				t.Fatal(err)
			}

			req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
			req.Header.Set("X-Ebeacon-Use-Upstream", "u1")
			rec := httptest.NewRecorder()
			n.ServeHTTP(rec, req)

			if rec.Code != tt.status {
				t.Fatalf("status: got %d want %d", rec.Code, tt.status)
			}
			if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("u1") {
				t.Fatalf("X-Ebeacon-Upstream: got %q want %q", got, ObfuscateUpstreamID("u1"))
			}
			if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"primary"}` {
				t.Fatalf("body: got %q", got)
			}
			if primaryHits.Load() != 1 {
				t.Fatalf("primary hits: got %d want 1", primaryHits.Load())
			}
			if backupHits.Load() != 0 {
				t.Fatalf("backup hits: got %d want 0", backupHits.Load())
			}
		})
	}
}

func TestNetwork_SelectedUpstreamUnavailableReturns503(t *testing.T) {
	var backupHits atomic.Int32
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":"backup"}`))
	}))
	defer backup.Close()

	id := netID(t)
	cfg := mustCfg(t, id, backup.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "u1", URL: "http://127.0.0.1:1"},
		{ID: "u2", URL: backup.URL},
	}
	cfg.Failsafe.Retry = &config.RetryConfig{MaxAttempts: 2, Delay: time.Millisecond, Backoff: 1, MaxDelay: time.Millisecond}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("X-Ebeacon-Use-Upstream", "u1")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if backupHits.Load() != 0 {
		t.Fatalf("backup hits: got %d want 0", backupHits.Load())
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != "" {
		t.Fatalf("unexpected upstream header: %q", got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != "selected upstream unavailable" {
		t.Fatalf("body: got %q", body)
	}
}

func TestNetwork_Upstream5xxRetriesToBackup(t *testing.T) {
	var primaryHits atomic.Int32
	var backupHits atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"primary"}`))
	}))
	defer primary.Close()

	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":"backup"}`))
	}))
	defer backup.Close()

	id := netID(t)
	cfg := mustCfg(t, id, primary.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "u1", URL: primary.URL},
		{ID: "u2", URL: backup.URL},
	}
	cfg.Failsafe.Retry = &config.RetryConfig{MaxAttempts: 2, Delay: time.Millisecond, Backoff: 1, MaxDelay: time.Millisecond}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("u2") {
		t.Fatalf("X-Ebeacon-Upstream: got %q want %q", got, ObfuscateUpstreamID("u2"))
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"data":"backup"}` {
		t.Fatalf("body: got %q", got)
	}
	if primaryHits.Load() != 1 {
		t.Fatalf("primary hits: got %d want 1", primaryHits.Load())
	}
	if backupHits.Load() != 1 {
		t.Fatalf("backup hits: got %d want 1", backupHits.Load())
	}
}

func TestNetwork_SelectedUpstream5xxReturnedWithoutRetry(t *testing.T) {
	var primaryHits atomic.Int32
	var backupHits atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"primary"}`))
	}))
	defer primary.Close()

	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":"backup"}`))
	}))
	defer backup.Close()

	id := netID(t)
	cfg := mustCfg(t, id, primary.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "u1", URL: primary.URL},
		{ID: "u2", URL: backup.URL},
	}
	cfg.Failsafe.Retry = &config.RetryConfig{MaxAttempts: 2, Delay: time.Millisecond, Backoff: 1, MaxDelay: time.Millisecond}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("X-Ebeacon-Use-Upstream", "u1")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("u1") {
		t.Fatalf("X-Ebeacon-Upstream: got %q want %q", got, ObfuscateUpstreamID("u1"))
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"primary"}` {
		t.Fatalf("body: got %q", got)
	}
	if primaryHits.Load() != 1 {
		t.Fatalf("primary hits: got %d want 1", primaryHits.Load())
	}
	if backupHits.Load() != 0 {
		t.Fatalf("backup hits: got %d want 0", backupHits.Load())
	}
}

func TestNetwork_All5xxResponsesBecome502(t *testing.T) {
	var primaryHits atomic.Int32
	var backupHits atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`primary failed`))
	}))
	defer primary.Close()

	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupHits.Add(1)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`backup failed`))
	}))
	defer backup.Close()

	id := netID(t)
	cfg := mustCfg(t, id, primary.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "u1", URL: primary.URL},
		{ID: "u2", URL: backup.URL},
	}
	cfg.Failsafe.Retry = &config.RetryConfig{MaxAttempts: 2, Delay: time.Millisecond, Backoff: 1, MaxDelay: time.Millisecond}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != "" {
		t.Fatalf("unexpected upstream header: %q", got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != "bad gateway" {
		t.Fatalf("body: got %q want %q", body, "bad gateway")
	}
	if primaryHits.Load() != 1 {
		t.Fatalf("primary hits: got %d want 1", primaryHits.Load())
	}
	if backupHits.Load() != 1 {
		t.Fatalf("backup hits: got %d want 1", backupHits.Load())
	}
}

func TestNetwork_HeaderSelectorByClientTypeDoesNotReuseDefaultCache(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"version":"from-a"}}`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
        priority: 0
      - id: b
        url: %q
        priority: 1
    routing:
      loadBalancing: round-robin
      stickySession: false
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
	n.Pool().ByID("b").SetClientType("teku")

	defaultRec := httptest.NewRecorder()
	n.ServeHTTP(defaultRec, httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil))
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("default status %d body %q", defaultRec.Code, defaultRec.Body.String())
	}
	if !strings.Contains(defaultRec.Body.String(), "from-a") {
		t.Fatalf("default body %q", defaultRec.Body.String())
	}

	selectedReq := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	selectedReq.Header.Set("X-Ebeacon-Use-Upstream", "client:teku")
	selectedRec := httptest.NewRecorder()
	n.ServeHTTP(selectedRec, selectedReq)
	if selectedRec.Code != http.StatusOK {
		t.Fatalf("selected status %d body %q", selectedRec.Code, selectedRec.Body.String())
	}
	if !strings.Contains(selectedRec.Body.String(), "from-b") {
		t.Fatalf("selected body %q", selectedRec.Body.String())
	}
	if got := selectedRec.Header().Get("X-Ebeacon-Cache"); got == "HIT" {
		t.Fatalf("selected client-type request should not reuse generic cache entry, got %q", got)
	}

	selectedHitReq := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	selectedHitReq.Header.Set("X-Ebeacon-Use-Upstream", "client:teku")
	selectedHitRec := httptest.NewRecorder()
	n.ServeHTTP(selectedHitRec, selectedHitReq)
	if got := selectedHitRec.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("expected selected client-type cache hit, got %q", got)
	}
	if !strings.Contains(selectedHitRec.Body.String(), "from-b") {
		t.Fatalf("selected hit body %q", selectedHitRec.Body.String())
	}
}

// TestNetwork_UseUpstreamHeaderBareClientType verifies that sending
// X-EBEACON-Use-Upstream: lighthouse (no "client:" prefix) routes to upstreams
// by client type, matching the behaviour of the path-based /lighthouse/eth/v1/...
// selector.
func TestNetwork_UseUpstreamHeaderBareClientType(t *testing.T) {
	var lighthouseHits, tekuHits atomic.Int32
	lh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lighthouseHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"version":"lighthouse"}}`))
	}))
	defer lh.Close()
	teku := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tekuHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"version":"teku"}}`))
	}))
	defer teku.Close()

	id := netID(t)
	cfg := mustCfg(t, id, lh.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "lh", URL: lh.URL},
		{ID: "tk", URL: teku.URL},
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	n.Pool().ByID("lh").SetClientType("lighthouse")
	n.Pool().ByID("tk").SetClientType("teku")

	// Bare "lighthouse" header should route to the lighthouse upstream, not 503.
	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("X-Ebeacon-Use-Upstream", "lighthouse")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "lighthouse") {
		t.Fatalf("expected lighthouse response, got %q", rec.Body.String())
	}
	if lighthouseHits.Load() != 1 {
		t.Fatalf("lighthouse hits: got %d want 1", lighthouseHits.Load())
	}
	if tekuHits.Load() != 0 {
		t.Fatalf("teku hits: got %d want 0", tekuHits.Load())
	}

	// "client:lighthouse" explicit prefix should behave identically.
	req2 := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req2.Header.Set("X-Ebeacon-Use-Upstream", "client:lighthouse")
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("client: prefix status: got %d body %q", rec2.Code, rec2.Body.String())
	}
	if lighthouseHits.Load() != 2 {
		t.Fatalf("lighthouse hits after client: prefix: got %d want 2", lighthouseHits.Load())
	}
}

func gunzipString(t *testing.T, payload []byte) string {
	t.Helper()
	gr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("read gzip response: %v", err)
	}
	out, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("decompress response: %v", err)
	}
	if err := gr.Close(); err != nil {
		t.Fatalf("close gzip response: %v", err)
	}
	return string(out)
}

func gunzipBytes(t *testing.T, payload []byte) []byte {
	t.Helper()
	gr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("read gzip response: %v", err)
	}
	out, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("decompress response: %v", err)
	}
	if err := gr.Close(); err != nil {
		t.Fatalf("close gzip response: %v", err)
	}
	return out
}
