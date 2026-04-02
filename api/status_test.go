package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ebeacon/ebeacon/config"
	networkpkg "github.com/ebeacon/ebeacon/network"
)

var statusLabelSeq atomic.Uint64

func statusNetworkID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("status_%d_%s", statusLabelSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "_"))
}

func TestStatusAPI_RegistersJSONAndUIRoutes(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	s := NewStatusAPI(map[string]*networkpkg.Network{})
	s.Auth = &config.AuthConfig{
		Keys: []config.APIKeyConfig{{ID: "test", Secret: "test-secret"}},
	}
	s.RegisterRoutes(mux, "/webui")

	recAPI := httptest.NewRecorder()
	reqAPI := httptest.NewRequest(http.MethodGet, "/webui/api/health", nil)
	reqAPI.Header.Set("Authorization", "Bearer test-secret")
	mux.ServeHTTP(recAPI, reqAPI)
	if recAPI.Code != http.StatusOK {
		t.Fatalf("health status: got %d", recAPI.Code)
	}
	if got := recAPI.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("health content-type: got %q", got)
	}

	recUI := httptest.NewRecorder()
	reqUI := httptest.NewRequest(http.MethodGet, "/webui/", nil)
	reqUI.Header.Set("Authorization", "Bearer test-secret")
	mux.ServeHTTP(recUI, reqUI)
	if recUI.Code != http.StatusOK {
		t.Fatalf("ui status: got %d", recUI.Code)
	}
	if got := recUI.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("ui content-type: got %q", got)
	}

	recUnauth := httptest.NewRecorder()
	reqUnauth := httptest.NewRequest(http.MethodGet, "/webui/api/health", nil)
	mux.ServeHTTP(recUnauth, reqUnauth)
	if recUnauth.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status: got %d", recUnauth.Code)
	}
}

func TestStatusAPI_PathTokenAuth(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	s := NewStatusAPI(map[string]*networkpkg.Network{})
	s.Auth = &config.AuthConfig{
		Keys: []config.APIKeyConfig{{ID: "webui", Secret: "webui-local-test-key"}},
	}
	s.RegisterRoutes(mux, "/webui")

	recAPI := httptest.NewRecorder()
	reqAPI := httptest.NewRequest(http.MethodGet, "/webui/webui-local-test-key/api/health", nil)
	mux.ServeHTTP(recAPI, reqAPI)
	if recAPI.Code != http.StatusOK {
		t.Fatalf("path token api status: got %d body %q", recAPI.Code, recAPI.Body.String())
	}
	if got := recAPI.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("path token api content-type: got %q", got)
	}

	recUI := httptest.NewRecorder()
	reqUI := httptest.NewRequest(http.MethodGet, "/webui/webui-local-test-key/", nil)
	mux.ServeHTTP(recUI, reqUI)
	if recUI.Code != http.StatusOK {
		t.Fatalf("path token ui status: got %d body %q", recUI.Code, recUI.Body.String())
	}
	if got := recUI.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("path token ui content-type: got %q", got)
	}

	recBad := httptest.NewRecorder()
	reqBad := httptest.NewRequest(http.MethodGet, "/webui/wrong-token/api/health", nil)
	mux.ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusUnauthorized {
		t.Fatalf("wrong path token status: got %d", recBad.Code)
	}
}

func TestStatusOrderingHelpers(t *testing.T) {
	t.Parallel()

	health := []networkHealth{{ID: "holesky"}, {ID: "mainnet"}, {ID: "sepolia"}}
	sortNetworkHealth(health)
	if got := []string{health[0].ID, health[1].ID, health[2].ID}; !reflect.DeepEqual(got, []string{"holesky", "mainnet", "sepolia"}) {
		t.Fatalf("health order: got %v", got)
	}

	cache := []cacheStats{{Network: "sepolia"}, {Network: "holesky"}, {Network: "mainnet"}}
	sortCacheStats(cache)
	if got := []string{cache[0].Network, cache[1].Network, cache[2].Network}; !reflect.DeepEqual(got, []string{"holesky", "mainnet", "sepolia"}) {
		t.Fatalf("cache order: got %v", got)
	}

	sessions := []sessionStats{{Network: "sepolia"}, {Network: "holesky"}, {Network: "mainnet"}}
	sortSessionStats(sessions)
	if got := []string{sessions[0].Network, sessions[1].Network, sessions[2].Network}; !reflect.DeepEqual(got, []string{"holesky", "mainnet", "sepolia"}) {
		t.Fatalf("sessions order: got %v", got)
	}

	upstreams := []upstreamStatus{
		{Network: "mainnet", Priority: 2, ID: "b"},
		{Network: "holesky", Priority: 3, ID: "z"},
		{Network: "mainnet", Priority: 1, ID: "a"},
		{Network: "mainnet", Priority: 1, ID: "c"},
	}
	sortUpstreamStatuses(upstreams)
	if got := []string{upstreams[0].Network + "/" + upstreams[0].ID, upstreams[1].Network + "/" + upstreams[1].ID, upstreams[2].Network + "/" + upstreams[2].ID, upstreams[3].Network + "/" + upstreams[3].ID}; !reflect.DeepEqual(got, []string{"holesky/z", "mainnet/a", "mainnet/c", "mainnet/b"}) {
		t.Fatalf("upstream order: got %v", got)
	}

	forks := []forkInfo{{Network: "sepolia"}, {Network: "holesky"}, {Network: "mainnet"}}
	sortForkInfos(forks)
	if got := []string{forks[0].Network, forks[1].Network, forks[2].Network}; !reflect.DeepEqual(got, []string{"holesky", "mainnet", "sepolia"}) {
		t.Fatalf("fork order: got %v", got)
	}

	forkUpstreams := []forkUpstream{{ID: "c", HeadSlot: 100}, {ID: "a", HeadSlot: 120}, {ID: "b", HeadSlot: 120}}
	sortForkUpstreams(forkUpstreams)
	if got := []string{forkUpstreams[0].ID, forkUpstreams[1].ID, forkUpstreams[2].ID}; !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("fork upstream order: got %v", got)
	}
}

func TestStatusAPI_CacheEntriesEndpoint(t *testing.T) {
	t.Parallel()
	networkID := statusNetworkID(t)
	network := mustStatusNetworkWithCache(t, networkID)
	cache := network.CacheInstance()
	if cache == nil {
		t.Fatal("expected cache instance")
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	body := []byte(`{"data":"cached"}`)
	cacheKey := networkID + ":GET:/eth/v1/node/version"
	cache.Set(cacheKey, http.StatusOK, headers, body, time.Minute)

	mux := http.NewServeMux()
	s := NewStatusAPI(map[string]*networkpkg.Network{networkID: network})
	s.Auth = &config.AuthConfig{Keys: []config.APIKeyConfig{{ID: "test", Secret: "test-secret"}}}
	s.RegisterRoutes(mux, "/webui")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/webui/api/cache/entries?network="+networkID, nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type: got %q", got)
	}
	var entries []cacheEntryInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &entries); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries len: got %d want 1", len(entries))
	}
	entry := entries[0]
	if entry.Network != networkID {
		t.Fatalf("network: got %q", entry.Network)
	}
	if entry.Key != cacheKey {
		t.Fatalf("key: got %q", entry.Key)
	}
	if entry.BodySize != len(body) {
		t.Fatalf("bodySize: got %d want %d", entry.BodySize, len(body))
	}
	if entry.BodyBase64 != "" {
		t.Fatalf("bodyBase64: got %q want empty", entry.BodyBase64)
	}
	if entry.ContentType != "application/json" {
		t.Fatalf("contentType: got %q", entry.ContentType)
	}
	if entry.ExpiresAt == nil {
		t.Fatal("expected expiresAt to be present")
	}
}

func TestStatusAPI_CacheEntriesEndpointIncludeBody(t *testing.T) {
	t.Parallel()
	networkID := statusNetworkID(t)
	network := mustStatusNetworkWithCache(t, networkID)
	cache := network.CacheInstance()
	if cache == nil {
		t.Fatal("expected cache instance")
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	body := []byte(`{"data":"cached"}`)
	cacheKey := networkID + ":GET:/eth/v1/node/version"
	cache.Set(cacheKey, http.StatusOK, headers, body, time.Minute)

	mux := http.NewServeMux()
	s := NewStatusAPI(map[string]*networkpkg.Network{networkID: network})
	s.Auth = &config.AuthConfig{Keys: []config.APIKeyConfig{{ID: "test", Secret: "test-secret"}}}
	s.RegisterRoutes(mux, "/webui")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/webui/api/cache/entries?network="+networkID+"&includeBody=true", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	var entries []cacheEntryInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &entries); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries len: got %d want 1", len(entries))
	}
	entry := entries[0]
	if entry.BodyBase64 != base64.StdEncoding.EncodeToString(body) {
		t.Fatalf("bodyBase64: got %q", entry.BodyBase64)
	}
	if entry.ContentType != "application/json" {
		t.Fatalf("contentType: got %q", entry.ContentType)
	}
	if entry.ExpiresAt == nil {
		t.Fatal("expected expiresAt to be present")
	}
}

func TestStatusAPI_CacheEntryDeleteEndpoint(t *testing.T) {
	t.Parallel()
	networkID := statusNetworkID(t)
	network := mustStatusNetworkWithCache(t, networkID)
	cache := network.CacheInstance()
	if cache == nil {
		t.Fatal("expected cache instance")
	}
	key := networkID + ":GET:/eth/v1/node/version"
	cache.Set(key, http.StatusOK, http.Header{"Content-Type": {"application/json"}}, []byte(`{"data":"cached"}`), time.Minute)

	mux := http.NewServeMux()
	s := NewStatusAPI(map[string]*networkpkg.Network{networkID: network})
	s.Auth = &config.AuthConfig{Keys: []config.APIKeyConfig{{ID: "test", Secret: "test-secret"}}}
	s.RegisterRoutes(mux, "/webui")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/webui/api/cache/entries?network="+networkID+"&key="+key, nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	var result cacheDeleteResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if !result.Deleted {
		t.Fatalf("deleted: got %#v", result)
	}
	if got := len(cache.Entries(10, false)); got != 0 {
		t.Fatalf("entries len after delete: got %d want 0", got)
	}

	badRec := httptest.NewRecorder()
	badReq := httptest.NewRequest(http.MethodDelete, "/webui/api/cache/entries?network="+networkID, nil)
	badReq.Header.Set("Authorization", "Bearer test-secret")
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("missing key status: got %d body %q", badRec.Code, badRec.Body.String())
	}
}

func TestStatusAPI_UpstreamsEndpointIncludesScoreDetails(t *testing.T) {
	t.Parallel()
	networkID := statusNetworkID(t)
	network := mustStatusNetwork(t, networkID, "score", false)
	pool := network.Pool()
	upstream := pool.ByID("u1")
	if upstream == nil {
		t.Fatal("expected upstream u1")
	}

	upstream.UpdateSyncStatus(false, 128, 0)
	upstream.UpdateHeadBlock(128, "0xabc", "0xdef")
	pool.BlockCache().AddBlock(upstream.ID, 128, "0xabc", "0xdef")
	upstream.RecordSuccess(25 * time.Millisecond)
	upstream.RecordSuccess(40 * time.Millisecond)
	pool.RefreshScoreMetrics()

	mux := http.NewServeMux()
	s := NewStatusAPI(map[string]*networkpkg.Network{networkID: network})
	s.Auth = &config.AuthConfig{Keys: []config.APIKeyConfig{{ID: "test", Secret: "test-secret"}}}
	s.RegisterRoutes(mux, "/webui")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/webui/api/upstreams", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	var upstreams []upstreamStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &upstreams); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if len(upstreams) != 1 {
		t.Fatalf("upstreams len: got %d want 1", len(upstreams))
	}
	item := upstreams[0]
	if item.LoadBalancing != "score" {
		t.Fatalf("loadBalancing: got %q want score", item.LoadBalancing)
	}
	if item.Score <= 0 {
		t.Fatalf("score: got %f want > 0", item.Score)
	}
	if item.ScoreSamples != 2 {
		t.Fatalf("scoreSamples: got %d want 2", item.ScoreSamples)
	}
	if item.ScoreP90LatencyMs <= 0 {
		t.Fatalf("scoreP90LatencyMs: got %f want > 0", item.ScoreP90LatencyMs)
	}
	if item.ScoreHeadLag != 0 {
		t.Fatalf("scoreHeadLag: got %d want 0", item.ScoreHeadLag)
	}
}

func mustStatusNetworkWithCache(t *testing.T, networkID string) *networkpkg.Network {
	t.Helper()
	return mustStatusNetwork(t, networkID, "round-robin", true)
}

func mustStatusNetwork(t *testing.T, networkID, loadBalancing string, cacheEnabled bool) *networkpkg.Network {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	yaml := fmt.Sprintf("logLevel: error\n"+
		"server: { host: \"127.0.0.1\", port: 5555, maxTimeout: 30s }\n"+
		"failsafe: { timeout: { duration: 10s } }\n"+
		"health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }\n"+
		"rateLimiting: {}\n"+
		"metrics: { enabled: false }\n"+
		"networks:\n"+
		"  - id: %q\n"+
		"    upstreams:\n"+
		"      - id: u1\n"+
		"        url: \"http://127.0.0.1:1\"\n"+
		"    routing:\n"+
		"      loadBalancing: %s\n"+
		"      stickySession: false\n"+
		"    cache:\n"+
		"      enabled: %t\n"+
		"      maxSize: 10\n", networkID, loadBalancing, cacheEnabled)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	network, err := networkpkg.New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	return network
}
