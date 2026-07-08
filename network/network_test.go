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

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/debuglog"
	"github.com/mysticryuujin/ebeacon/upstream"
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

	// Path references slot 50 — finalized (50 <= 100*32).
	ttl := n.effectiveCacheTTL(time.Minute, "/eth/v2/beacon/blocks/50")
	if ttl != 0 {
		t.Fatalf("expected forever (0) for finalized slot, got %v", ttl)
	}

	// The checkpoint boundary slot itself is finalized...
	ttl = n.effectiveCacheTTL(time.Minute, "/eth/v2/beacon/blocks/3200")
	if ttl != 0 {
		t.Fatalf("expected forever (0) for checkpoint boundary slot, got %v", ttl)
	}

	// ...but the next slot is only justified and can still reorg.
	ttl = n.effectiveCacheTTL(time.Minute, "/eth/v2/beacon/blocks/3201")
	if ttl != time.Minute {
		t.Fatalf("expected policy TTL for slot past the checkpoint, got %v", ttl)
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

	ttl := n.effectiveCacheTTL(30*time.Second, "/eth/v1/beacon/rewards/attestations/98")
	if ttl != 0 {
		t.Fatalf("expected forever (0) for finalized epoch rewards, got %v", ttl)
	}

	// Rewards for epoch 99 depend on inclusions in epoch 100, which is not
	// yet fully finalized at checkpoint epoch 100.
	ttl = n.effectiveCacheTTL(30*time.Second, "/eth/v1/beacon/rewards/attestations/99")
	if ttl != 30*time.Second {
		t.Fatalf("expected policy TTL for epoch within finality window, got %v", ttl)
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
	req1.Header.Set("Accept", "application/octet-stream")
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
	req2.Header.Set("Accept", "application/octet-stream")
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

func TestNetwork_CachedOctetStreamNotGzippedEvenIfClientAcceptsGzip(t *testing.T) {
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

	// Second request: same SSZ key, client also sends Accept-Encoding: gzip.
	// The proxy must NOT gzip SSZ responses — binary payloads compress poorly
	// and CL clients expect raw bytes without a transport re-encoding step.
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
	if got := rec2.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding should be empty for SSZ, got %q", got)
	}
	if !bytes.Equal(rec2.Body.Bytes(), want) {
		t.Fatalf("cached binary body mismatch: got %d want %d", len(rec2.Body.Bytes()), len(want))
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

	genericKey := dedupKey("mainnet", http.MethodGet, u, nil, "", false)
	clientKey := dedupKey("mainnet", http.MethodGet, u, nil, "client:nimbus", false)

	if genericKey == clientKey {
		t.Fatalf("expected selector-scoped dedup key to differ, got %q", genericKey)
	}
}

func TestDedupKey_IncludesAcceptRepresentation(t *testing.T) {
	u := &url.URL{Path: "/eth/v2/beacon/blocks/12345"}

	jsonKey := dedupKey("mainnet", http.MethodGet, u, nil, "", false)
	sszKey := dedupKey("mainnet", http.MethodGet, u, nil, "", true)

	if jsonKey == sszKey {
		t.Fatalf("SSZ and JSON requests must not share a dedup key, got %q", jsonKey)
	}
}

func TestAcceptPrefersSSZ(t *testing.T) {
	t.Parallel()
	tests := []struct {
		accept string
		want   bool
	}{
		{"", false},
		{"application/octet-stream", true},
		{"Application/Octet-Stream", true},
		{"application/json", false},
		{"application/octet-stream;q=1.0,application/json;q=0.9", true},
		{"application/octet-stream; q=1.0, application/json; q=0.9", true},
		{"application/json;q=1.0,application/octet-stream;q=0.9", false},
		{"application/octet-stream;q=0", false},
		{"*/*", false},
		{"application/*", false},
		{"application/octet-stream, application/json", false},
		{"application/octet-stream;q=0.5, */*;q=0.1", true},
		{"text/html", false},
		{"application/octet-stream;q=garbage", true},
	}
	for _, tt := range tests {
		if got := acceptPrefersSSZ(tt.accept); got != tt.want {
			t.Errorf("acceptPrefersSSZ(%q) = %v, want %v", tt.accept, got, tt.want)
		}
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

func TestPathHasNamedSlotID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		path string
		want bool
	}{
		// bare /headers implies head
		{"/eth/v1/beacon/headers", true},
		// named block IDs
		{"/eth/v1/beacon/headers/head", true},
		{"/eth/v1/beacon/headers/finalized", true},
		{"/eth/v1/beacon/headers/justified", true},
		{"/eth/v2/beacon/blocks/head", true},
		{"/eth/v1/beacon/blobs/head", true},
		{"/eth/v1/beacon/blob_sidecars/head", true},
		{"/eth/v1/beacon/blinded_blocks/finalized", true},
		{"/eth/v1/beacon/states/head/validators", true},
		{"/eth/v1/beacon/states/finalized/root", true},
		{"/eth/v1/beacon/states/justified/finality_checkpoints", true},
		// numeric slot IDs — not named
		{"/eth/v1/beacon/headers/12345", false},
		{"/eth/v1/beacon/blocks/12345", false},
		{"/eth/v1/beacon/blobs/12345", false},
		{"/eth/v1/beacon/states/12345/validators", false},
		// genesis is not in the named set (it's a known constant, handled separately)
		{"/eth/v1/beacon/headers/genesis", false},
		// unrelated paths
		{"/eth/v1/node/syncing", false},
		{"/eth/v1/config/genesis", false},
		{"/eth/v1/validator/duties/proposer/123", false},
	}
	for _, tt := range tests {
		if got := pathHasNamedSlotID(tt.path); got != tt.want {
			t.Errorf("pathHasNamedSlotID(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestEffectiveCacheTTL_SlotAlignment(t *testing.T) {
	t.Parallel()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)

	// Build a network with genesisTime set to mainnet genesis.
	cfgWith := mustCfgText(t, fmt.Sprintf(`
logLevel: error
server: { host: "127.0.0.1", port: 5555, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: %s
    genesisTime: 1606824023
    upstreams:
      - id: u1
        url: %q
    cache: { enabled: false }
`, id, up.URL))
	nWith, err := New(&cfgWith.Networks[0], cfgWith)
	if err != nil {
		t.Fatal(err)
	}

	// With genesisTime, named slot paths must get a TTL in (0, policyTTL].
	ttl := nWith.effectiveCacheTTL(4*time.Second, "/eth/v1/beacon/headers")
	if ttl <= 0 || ttl > 4*time.Second {
		t.Errorf("named path with genesisTime: expected 0 < ttl <= 4s, got %v", ttl)
	}
	ttl2 := nWith.effectiveCacheTTL(4*time.Second, "/eth/v1/beacon/headers/head")
	if ttl2 <= 0 || ttl2 > 4*time.Second {
		t.Errorf("named path /head with genesisTime: expected 0 < ttl <= 4s, got %v", ttl2)
	}

	// Numeric slot paths must pass through unchanged (no slot alignment).
	if got := nWith.effectiveCacheTTL(12*time.Second, "/eth/v1/beacon/headers/12345"); got != 12*time.Second {
		t.Errorf("numeric slot with genesisTime: expected 12s passthrough, got %v", got)
	}

	// Build a network without genesisTime — TTL must pass through unchanged.
	id2 := netID(t)
	cfgWithout := mustCfgText(t, fmt.Sprintf(`
logLevel: error
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
    cache: { enabled: false }
`, id2, up.URL))
	nWithout, err := New(&cfgWithout.Networks[0], cfgWithout)
	if err != nil {
		t.Fatal(err)
	}
	if got := nWithout.effectiveCacheTTL(4*time.Second, "/eth/v1/beacon/headers"); got != 4*time.Second {
		t.Errorf("named path without genesisTime: expected 4s passthrough, got %v", got)
	}
}

func TestCacheKeyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		key  string
		want string
	}{
		{"mainnet:GET:/eth/v1/beacon/headers", "/eth/v1/beacon/headers"},
		{"mainnet:GET:/eth/v1/beacon/headers/head", "/eth/v1/beacon/headers/head"},
		{"mainnet:GET:/eth/v1/beacon/headers/12345", "/eth/v1/beacon/headers/12345"},
		{"mainnet:GET:/eth/v1/beacon/headers/head:upstream=client:lighthouse", "/eth/v1/beacon/headers/head"},
		{"mainnet:GET:/eth/v1/beacon/headers:accept=binary", "/eth/v1/beacon/headers"},
		{"mainnet:GET:/eth/v1/beacon/headers:accept=binary:upstream=u1", "/eth/v1/beacon/headers"},
		{"mainnet:GET:/eth/v1/node/syncing?slot=1", "/eth/v1/node/syncing"},
		{"mainnet:HEAD:/eth/v1/beacon/headers/head", "/eth/v1/beacon/headers/head"},
	}
	for _, tt := range tests {
		if got := cacheKeyPath(tt.key); got != tt.want {
			t.Errorf("cacheKeyPath(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestIsNamedHeadCacheKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		key  string
		want bool
	}{
		{"mainnet:GET:/eth/v1/beacon/headers", true},
		{"mainnet:GET:/eth/v1/beacon/headers/head", true},
		{"mainnet:GET:/eth/v1/beacon/headers/finalized", true},
		{"mainnet:GET:/eth/v1/beacon/headers/justified", true},
		{"mainnet:GET:/eth/v1/beacon/blocks/head:upstream=u1", true},
		{"mainnet:GET:/eth/v1/beacon/states/head/validators", true},
		{"mainnet:GET:/eth/v1/beacon/headers/12345", false},
		{"mainnet:GET:/eth/v1/beacon/blocks/99999", false},
		{"mainnet:GET:/eth/v1/node/syncing", false},
	}
	for _, tt := range tests {
		if got := isNamedHeadCacheKey(tt.key); got != tt.want {
			t.Errorf("isNamedHeadCacheKey(%q) = %v, want %v", tt.key, got, tt.want)
		}
	}
}

func TestHeadWatcher_InvalidatesOnHeadEvent(t *testing.T) {
	t.Parallel()

	// Track whether PurgeIf was called.
	purged := make(chan int, 4)

	// Fake upstream SSE server that sends one head event then keeps the
	// connection open until the client disconnects.
	sseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		fmt.Fprint(w, "event: head\ndata: {\"slot\":\"100\"}\n\n") //nolint:errcheck
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Hold connection open until client disconnects.
		<-r.Context().Done()
	}))
	defer sseServer.Close()

	id := netID(t)
	cfg := mustCfgText(t, fmt.Sprintf(`
logLevel: error
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
    cache: { enabled: true, maxSize: 32 }
`, id, sseServer.URL))

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Seed the cache with a named-head entry.
	n.cache.Set(id+":GET:/eth/v1/beacon/headers/head", 200,
		http.Header{"Content-Type": []string{"application/json"}},
		[]byte(`{}`), 4*time.Second)
	// And a numeric entry that must NOT be purged.
	n.cache.Set(id+":GET:/eth/v1/beacon/headers/12345", 200,
		http.Header{"Content-Type": []string{"application/json"}},
		[]byte(`{}`), 12*time.Second)

	if n.cache.Size() != 2 {
		t.Fatalf("expected 2 pre-seeded cache entries, got %d", n.cache.Size())
	}

	// Wrap PurgeIf to capture calls.
	origPurge := n.cache

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the watcher.
	w := startHeadWatcher(ctx, id, n.pool, origPurge, nil)

	// Wait for the head event to be processed (max 3 s).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n.cache.Get(id+":GET:/eth/v1/beacon/headers/head") == nil {
			purged <- 1
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	w.wait()

	select {
	case <-purged:
		// head entry was invalidated — pass
	default:
		t.Error("head cache entry was not invalidated after head event")
	}

	// Numeric entry must still be present.
	if n.cache.Get(id+":GET:/eth/v1/beacon/headers/12345") == nil {
		t.Error("numeric slot cache entry was unexpectedly invalidated")
	}
}

func TestNetwork_POSTBodyForwardedWithoutRetryConfig(t *testing.T) {
	var gotBody []byte
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	if cfg.Failsafe.Retry != nil || cfg.Failsafe.Hedge != nil {
		t.Fatal("test requires a config without retry/hedge")
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	body := `["1","2","3"]`
	req := httptest.NewRequest(http.MethodPost, "/eth/v1/beacon/states/head/validators", strings.NewReader(body))
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d %q", rec.Code, rec.Body.String())
	}
	if string(gotBody) != body {
		t.Fatalf("upstream body: got %q want %q", gotBody, body)
	}
}

func TestNetwork_POSTBodyTooLargeReturns413(t *testing.T) {
	upstreamCalled := false
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/eth/v1/beacon/states/head/validators",
		io.LimitReader(neverEnding('x'), 32<<20+1))
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: got %d want 413", rec.Code)
	}
	if upstreamCalled {
		t.Fatal("oversized body must not reach the upstream")
	}
}

type neverEnding byte

func (b neverEnding) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}

func TestReadBodyCapped_EnforcesLimit(t *testing.T) {
	t.Parallel()
	body, err := readBodyCapped(strings.NewReader("12345"), 5)
	if err != nil || string(body) != "12345" {
		t.Fatalf("exact-limit read failed: %v %q", err, body)
	}
	if _, err := readBodyCapped(strings.NewReader("123456"), 5); err == nil {
		t.Fatal("expected error for body exceeding limit")
	}
	body, err = readBodyCapped(strings.NewReader("123456"), 0)
	if err != nil || string(body) != "123456" {
		t.Fatalf("limit 0 must mean unlimited: %v %q", err, body)
	}
}

func TestNetwork_OversizedUpstreamResponseReturns502(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte("x"), 1024)) //nolint:errcheck
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Server.MaxResponseBodyBytes = 512
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status: got %d want 502", rec.Code)
	}
}

func TestNetwork_HEADPreservesUpstreamContentLength(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "12345")
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodHead, "/eth/v1/node/version", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Length"); got != "12345" {
		t.Fatalf("Content-Length: got %q want 12345", got)
	}
}

func TestNetwork_SpecAcceptHeaderKeepsRepresentationsSeparate(t *testing.T) {
	sszBody := []byte{0x0a, 0x0b, 0x0c, 0x0d}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Emulate a CL node honoring q-values: any Accept mentioning
		// octet-stream first is served SSZ.
		if strings.Contains(r.Header.Get("Accept"), "application/octet-stream") {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(sszBody)
			return
		}
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

	// The beacon-API spec's recommended SSZ Accept header (q-valued).
	sszReq := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/456", nil)
	sszReq.Header.Set("Accept", "application/octet-stream;q=1.0,application/json;q=0.9")
	sszRec := httptest.NewRecorder()
	n.ServeHTTP(sszRec, sszReq)
	if sszRec.Code != http.StatusOK || !bytes.Equal(sszRec.Body.Bytes(), sszBody) {
		t.Fatalf("ssz response: %d %q", sszRec.Code, sszRec.Body.Bytes())
	}

	jsonReq := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/456", nil)
	jsonRec := httptest.NewRecorder()
	n.ServeHTTP(jsonRec, jsonReq)
	if got := jsonRec.Body.String(); got != `{"data":"json"}` {
		t.Fatalf("json client received wrong representation: %q", got)
	}

	// A second q-valued SSZ request must hit the binary-keyed cache entry.
	sszReq2 := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/456", nil)
	sszReq2.Header.Set("Accept", "application/octet-stream;q=1.0,application/json;q=0.9")
	sszRec2 := httptest.NewRecorder()
	n.ServeHTTP(sszRec2, sszReq2)
	if got := sszRec2.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("second ssz request should be a cache hit, got %q", got)
	}
	if !bytes.Equal(sszRec2.Body.Bytes(), sszBody) {
		t.Fatalf("cached ssz body: %q", sszRec2.Body.Bytes())
	}
}

func TestNetwork_MismatchedRepresentationIsNotCached(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Misbehaving upstream: returns SSZ regardless of Accept.
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte{0x01, 0x02})
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

	jsonReq := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/456", nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, jsonReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}

	jsonReq2 := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blobs/456", nil)
	rec2 := httptest.NewRecorder()
	n.ServeHTTP(rec2, jsonReq2)
	if got := rec2.Header().Get("X-Ebeacon-Cache"); got == "HIT" {
		t.Fatal("SSZ body must not be cached under a JSON-keyed entry")
	}
}

func TestNetwork_HedgeLoserConnectionsReturnToZero(t *testing.T) {
	bothArrived := make(chan struct{})
	var arrived atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if arrived.Add(1) == 2 {
			close(bothArrived)
		}
		select {
		case <-bothArrived:
		case <-time.After(2 * time.Second):
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":"ok"}`))
	})
	upA := httptest.NewServer(handler)
	defer upA.Close()
	upB := httptest.NewServer(handler)
	defer upB.Close()

	id := netID(t)
	cfg := mustCfg(t, id, upA.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "u1", URL: upA.URL},
		{ID: "u2", URL: upB.URL},
	}
	cfg.Failsafe.Hedge = &config.HedgeConfig{Delay: time.Millisecond, MaxCount: 1}

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

	u1, u2 := n.pool.ByID("u1"), n.pool.ByID("u2")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if u1.ActiveConns() == 0 && u2.ActiveConns() == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("hedge loser leaked active connections: u1=%d u2=%d",
				u1.ActiveConns(), u2.ActiveConns())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNetwork_ClientCancelDoesNotTripCircuitBreaker(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Failsafe.CircuitBreaker = &config.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		HalfOpenAfter:    time.Hour,
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/eth/v1/beacon/states/head/validators",
		strings.NewReader(`["1"]`)).WithContext(cctx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.ServeHTTP(rec, req)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if u := n.pool.ByID("u1"); !u.IsReady() {
		t.Fatal("client disconnect must not trip the circuit breaker")
	}
}

func TestNetwork_FailsafeTimeoutStillTripsCircuitBreaker(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	cfg.Failsafe.Timeout = &config.TimeoutConfig{Duration: 50 * time.Millisecond}
	cfg.Failsafe.CircuitBreaker = &config.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		HalfOpenAfter:    time.Hour,
	}
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/eth/v1/beacon/states/head/validators",
		strings.NewReader(`["1"]`))
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status: got %d want 502", rec.Code)
	}
	if u := n.pool.ByID("u1"); u.IsReady() {
		t.Fatal("failsafe timeout must still count as a circuit breaker failure")
	}
}

func TestNetwork_MultiplexLeaderDisconnectDoesNotFailFollowers(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		close(arrived)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":"ok"}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil).WithContext(leaderCtx)
		n.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-arrived

	followerRec := httptest.NewRecorder()
	followerDone := make(chan struct{})
	go func() {
		defer close(followerDone)
		req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
		n.ServeHTTP(followerRec, req)
	}()
	time.Sleep(50 * time.Millisecond)

	cancelLeader()
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader handler did not return after client disconnect")
	}

	close(release)
	select {
	case <-followerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("follower did not complete")
	}
	if followerRec.Code != http.StatusOK {
		t.Fatalf("follower status: got %d want 200 (leader disconnect must not fail followers)", followerRec.Code)
	}
	if hits.Load() != 1 {
		t.Fatalf("upstream hits: got %d want 1 (execution must stay shared)", hits.Load())
	}
}

func TestNetwork_MultiplexFollowerCancelReturnsPromptly(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":"ok"}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	leaderRec := httptest.NewRecorder()
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
		n.ServeHTTP(leaderRec, req)
	}()
	<-arrived

	followerCtx, cancelFollower := context.WithCancel(context.Background())
	followerDone := make(chan struct{})
	go func() {
		defer close(followerDone)
		req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil).WithContext(followerCtx)
		n.ServeHTTP(httptest.NewRecorder(), req)
	}()
	time.Sleep(50 * time.Millisecond)

	cancelFollower()
	select {
	case <-followerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("follower must return promptly when its own client cancels")
	}

	close(release)
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not complete")
	}
	if leaderRec.Code != http.StatusOK {
		t.Fatalf("leader status: got %d want 200", leaderRec.Code)
	}
}

func TestForward_AppendsRealPeerToXFF(t *testing.T) {
	var gotXFF string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotXFF = r.Header.Get("X-Forwarded-For")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.RemoteAddr = "192.0.2.10:5555"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if gotXFF != "1.2.3.4, 192.0.2.10" {
		t.Fatalf("upstream XFF chain must end with the real peer, got %q", gotXFF)
	}
}

func TestRequiredSelectorFromValue_GlobPrefixRoundTrip(t *testing.T) {
	t.Parallel()
	sel := requiredUpstreamSelector{glob: "*lighthouse*"}
	parsed := requiredSelectorFromValue(sel.label())
	if parsed.glob != "*lighthouse*" || parsed.upstreamID != "" || parsed.clientType != "" {
		t.Fatalf("glob scope label must round-trip, got %+v", parsed)
	}
}

func TestForward_PreservesPercentEncodedPath(t *testing.T) {
	var gotURI string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.RequestURI
		_, _ = w.Write([]byte(`{}`))
	}))
	defer up.Close()

	id := netID(t)
	cfg := mustCfg(t, id, up.URL, nil)
	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	target := "/eth/v1/x/head%3Ffoo"
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if gotURI != target {
		t.Fatalf("upstream URI: got %q want %q (encoded segment content must survive)", gotURI, target)
	}
}
