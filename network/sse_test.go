package network

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/upstream"
)

func TestSSERelay_StreamsLargeEventAndSetsHeaders(t *testing.T) {
	topicPayload := strings.Repeat("x", 256<<10)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "event: head\n")              //nolint:errcheck
		fmt.Fprintf(w, "data: %s\n\n", topicPayload) //nolint:errcheck
	}))
	defer up.Close()

	pool, err := upstream.NewPool(
		netID(t),
		[]config.UpstreamConfig{{ID: "u1", URL: up.URL}},
		config.RoutingConfig{LoadBalancing: "round-robin"},
		config.HealthConfig{CheckInterval: time.Hour, FinalityInterval: time.Hour, MaxSyncDistance: 10},
		nil,
	)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	relay := newSSERelay("mainnet", pool)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/events?topics=head", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()

	relay.Serve(rec, req, "", requiredUpstreamSelector{})

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content-type: got %q", got)
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("u1") {
		t.Fatalf("upstream header: got %q", got)
	}
	if !strings.Contains(rec.Body.String(), topicPayload) {
		t.Fatal("large SSE payload missing from response body")
	}
}

func TestNetwork_SSEClientRoutesStayIsolated(t *testing.T) {
	makeSSEUpstream := func(label string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			fmt.Fprintf(w, "event: head\n")              //nolint:errcheck
			fmt.Fprintf(w, "data: %s-stream\n\n", label) //nolint:errcheck
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		}))
	}

	nimbus := makeSSEUpstream("nimbus")
	defer nimbus.Close()
	caplin := makeSSEUpstream("caplin")
	defer caplin.Close()

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
      - id: nimbus
        url: %q
      - id: caplin
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/nimbus/"
          upstreamId: nimbus
        - pathPrefix: "/caplin/"
          upstreamId: caplin
    cache:
      enabled: false
`, id, nimbus.URL, caplin.URL)
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

	type result struct {
		name string
		rec  *httptest.ResponseRecorder
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup

	runClient := func(name, path string) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
		req.Header.Set("Accept", "text/event-stream")
		rec := httptest.NewRecorder()
		n.ServeHTTP(rec, req)
		results <- result{name: name, rec: rec}
	}

	wg.Add(2)
	go runClient("nimbus", "/nimbus/eth/v1/events?topics=head")
	go runClient("caplin", "/caplin/eth/v1/events?topics=head")
	wg.Wait()
	close(results)

	got := map[string]*httptest.ResponseRecorder{}
	for res := range results {
		got[res.name] = res.rec
	}

	nimbusRec := got["nimbus"]
	caplinRec := got["caplin"]
	if nimbusRec == nil || caplinRec == nil {
		t.Fatalf("missing SSE results: nimbus=%v caplin=%v", nimbusRec != nil, caplinRec != nil)
	}

	if got := nimbusRec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("nimbus") {
		t.Fatalf("nimbus upstream header: got %q", got)
	}
	if got := caplinRec.Header().Get("X-Ebeacon-Upstream"); got != ObfuscateUpstreamID("caplin") {
		t.Fatalf("caplin upstream header: got %q", got)
	}

	nimbusBody := nimbusRec.Body.String()
	caplinBody := caplinRec.Body.String()
	if !strings.Contains(nimbusBody, "nimbus-stream") {
		t.Fatalf("nimbus body missing own event: %q", nimbusBody)
	}
	if strings.Contains(nimbusBody, "caplin-stream") {
		t.Fatalf("nimbus body contains caplin event: %q", nimbusBody)
	}
	if !strings.Contains(caplinBody, "caplin-stream") {
		t.Fatalf("caplin body missing own event: %q", caplinBody)
	}
	if strings.Contains(caplinBody, "nimbus-stream") {
		t.Fatalf("caplin body contains nimbus event: %q", caplinBody)
	}
}

func TestNetwork_SSEClientRouteUnavailableReturns503(t *testing.T) {
	nimbusHits := 0
	nimbus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nimbusHits++
		http.Error(w, "nimbus unavailable", http.StatusServiceUnavailable)
	}))
	defer nimbus.Close()

	caplinHits := 0
	caplin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caplinHits++
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "event: head\n")           //nolint:errcheck
		fmt.Fprintf(w, "data: caplin-stream\n\n") //nolint:errcheck
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer caplin.Close()

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
      - id: nimbus
        url: %q
      - id: caplin
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
      clientRoutes:
        - pathPrefix: "/nimbus/"
          upstreamId: nimbus
        - pathPrefix: "/caplin/"
          upstreamId: caplin
    cache:
      enabled: false
`, id, nimbus.URL, caplin.URL)
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

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/nimbus/eth/v1/events?topics=head", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if nimbusHits == 0 {
		t.Fatal("expected selected upstream to be attempted")
	}
	if caplinHits != 0 {
		t.Fatalf("caplin hits: got %d want 0", caplinHits)
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != "" {
		t.Fatalf("unexpected upstream header: %q", got)
	}
	if !strings.Contains(rec.Body.String(), "selected upstream unavailable") {
		t.Fatalf("body %q", rec.Body.String())
	}
}

func TestNetwork_SSEHeaderSelectorUnavailableReturns503(t *testing.T) {
	nimbusHits := 0
	nimbus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nimbusHits++
		http.Error(w, "nimbus unavailable", http.StatusServiceUnavailable)
	}))
	defer nimbus.Close()

	caplinHits := 0
	caplin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caplinHits++
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "event: head\n")           //nolint:errcheck
		fmt.Fprintf(w, "data: caplin-stream\n\n") //nolint:errcheck
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer caplin.Close()

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
      - id: nimbus
        url: %q
      - id: caplin
        url: %q
    routing:
      loadBalancing: round-robin
      stickySession: false
    cache:
      enabled: false
`, id, nimbus.URL, caplin.URL)
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

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/eth/v1/events?topics=head", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Ebeacon-Use-Upstream", "nimbus")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if nimbusHits == 0 {
		t.Fatal("expected selected upstream to be attempted")
	}
	if caplinHits != 0 {
		t.Fatalf("caplin hits: got %d want 0", caplinHits)
	}
	if got := rec.Header().Get("X-Ebeacon-Upstream"); got != "" {
		t.Fatalf("unexpected upstream header: %q", got)
	}
	if !strings.Contains(rec.Body.String(), "selected upstream unavailable") {
		t.Fatalf("body %q", rec.Body.String())
	}
}

func TestSSERelay_ReconnectsToBackupAfterDisconnect(t *testing.T) {
	var primaryHits int
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHits++
		switch primaryHits {
		case 1:
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "event: head\n")           //nolint:errcheck
			fmt.Fprintf(w, "data: primary-first\n\n") //nolint:errcheck
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			// Returning closes the stream and forces the relay to reconnect.
		default:
			http.Error(w, "primary unavailable", http.StatusServiceUnavailable)
		}
	}))
	defer primary.Close()

	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "event: head\n")           //nolint:errcheck
		fmt.Fprintf(w, "data: backup-second\n\n") //nolint:errcheck
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer backup.Close()

	pool, err := upstream.NewPool(
		netID(t),
		[]config.UpstreamConfig{
			{
				ID:  "primary",
				URL: primary.URL,
				Failsafe: &config.FailsafeConfig{
					CircuitBreaker: &config.CircuitBreakerConfig{
						FailureThreshold: 1,
						SuccessThreshold: 1,
						HalfOpenAfter:    time.Hour,
					},
				},
			},
			{
				ID:  "backup",
				URL: backup.URL,
				Failsafe: &config.FailsafeConfig{
					CircuitBreaker: &config.CircuitBreakerConfig{
						FailureThreshold: 1,
						SuccessThreshold: 1,
						HalfOpenAfter:    time.Hour,
					},
				},
			},
		},
		config.RoutingConfig{LoadBalancing: "round-robin"},
		config.HealthConfig{CheckInterval: time.Hour, FinalityInterval: time.Hour, MaxSyncDistance: 10},
		nil,
	)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	relay := newSSERelay("mainnet", pool)
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/events?topics=head", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()

	relay.Serve(rec, req, "primary", requiredUpstreamSelector{})

	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, body)
	}
	if !strings.Contains(body, "primary-first") {
		t.Fatalf("body missing primary event: %q", body)
	}
	if !strings.Contains(body, "backup-second") {
		t.Fatalf("body missing backup event after reconnect: %q", body)
	}
	if primaryHits < 2 {
		t.Fatalf("expected relay to retry primary before failing over, got %d hits", primaryHits)
	}
}

func TestSSERelay_SendsPingCommentsDuringIdlePeriods(t *testing.T) {
	prev := ssePingInterval
	ssePingInterval = 20 * time.Millisecond
	defer func() { ssePingInterval = prev }()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer up.Close()

	pool, err := upstream.NewPool(
		netID(t),
		[]config.UpstreamConfig{{ID: "u1", URL: up.URL}},
		config.RoutingConfig{LoadBalancing: "round-robin"},
		config.HealthConfig{CheckInterval: time.Hour, FinalityInterval: time.Hour, MaxSyncDistance: 10},
		nil,
	)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	relay := newSSERelay("mainnet", pool)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/events?topics=head", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()

	relay.Serve(rec, req, "", requiredUpstreamSelector{})

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), ": ping") {
		t.Fatalf("expected ping comment in stream body, got %q", rec.Body.String())
	}
}

func TestSSERelay_StopsPromptlyOnClientDisconnect(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer up.Close()

	pool, err := upstream.NewPool(
		netID(t),
		[]config.UpstreamConfig{{ID: "u1", URL: up.URL}},
		config.RoutingConfig{LoadBalancing: "round-robin"},
		config.HealthConfig{CheckInterval: time.Hour, FinalityInterval: time.Hour, MaxSyncDistance: 10},
		nil,
	)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	relay := newSSERelay("mainnet", pool)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/events?topics=head", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()

	start := time.Now()
	relay.Serve(rec, req, "", requiredUpstreamSelector{})
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("relay did not stop promptly on client disconnect: %v", elapsed)
	}
}

func TestSSERelay_FlushesTerminalEventWithoutBlankLine(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "event: head\ndata: trailing-event") //nolint:errcheck
	}))
	defer up.Close()

	pool, err := upstream.NewPool(
		netID(t),
		[]config.UpstreamConfig{{ID: "u1", URL: up.URL}},
		config.RoutingConfig{LoadBalancing: "round-robin"},
		config.HealthConfig{CheckInterval: time.Hour, FinalityInterval: time.Hour, MaxSyncDistance: 10},
		nil,
	)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	relay := newSSERelay("mainnet", pool)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/events?topics=head", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()

	relay.Serve(rec, req, "", requiredUpstreamSelector{})

	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, body)
	}
	if !strings.Contains(body, "retry: 1000") {
		t.Fatalf("stream missing retry directive: %q", body)
	}
	if !strings.Contains(body, "data: trailing-event") {
		t.Fatalf("stream missing terminal event payload: %q", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("stream missing terminating blank line: %q", body)
	}
}

func TestSSERelay_ReconnectsOnIdleUpstream(t *testing.T) {
	oldIdle := sseIdleTimeout
	sseIdleTimeout = 100 * time.Millisecond
	defer func() { sseIdleTimeout = oldIdle }()

	var mu sync.Mutex
	connects := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		connects++
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Go silent: never send an event, never close.
		<-r.Context().Done()
	}))
	defer up.Close()

	pool, err := upstream.NewPool(
		netID(t),
		[]config.UpstreamConfig{{ID: "u1", URL: up.URL}},
		config.RoutingConfig{LoadBalancing: "round-robin"},
		config.HealthConfig{CheckInterval: time.Hour, FinalityInterval: time.Hour, MaxSyncDistance: 10},
		nil,
	)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	relay := newSSERelay("mainnet", pool)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/events?topics=head", nil).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()
	relay.Serve(rec, req, "", requiredUpstreamSelector{})

	mu.Lock()
	defer mu.Unlock()
	if connects < 2 {
		t.Fatalf("silent upstream must trigger reconnects, got %d connects", connects)
	}
}

func TestReadCappedLine_NewlineFreeStreamBailsAtCap(t *testing.T) {
	t.Parallel()
	// A reader that yields 'x' forever, never a newline, must not accumulate
	// without bound: readCappedLine bails once it exceeds the cap.
	const capBytes = 4096
	inf := bufio.NewReader(neverEndingReader('x'))
	line, err := readCappedLine(inf, capBytes)
	if err != errSSELineTooLong {
		t.Fatalf("expected errSSELineTooLong, got %v", err)
	}
	if len(line) > capBytes+64<<10 {
		t.Fatalf("capped line grew too large: %d bytes", len(line))
	}
}

func TestReadCappedLine_NormalLine(t *testing.T) {
	t.Parallel()
	r := bufio.NewReader(strings.NewReader("data: hello\n"))
	line, err := readCappedLine(r, 4096)
	if err != nil || line != "data: hello\n" {
		t.Fatalf("got %q, %v", line, err)
	}
}

type neverEndingReader byte

func (b neverEndingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}
