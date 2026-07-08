package network

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/upstream"
)

func mkConsensusUpstream(id, rawURL string) *upstream.Upstream {
	return upstream.New("testnet", config.UpstreamConfig{ID: id, URL: rawURL}, &config.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		HalfOpenAfter:    time.Second,
	}, 100)
}

func TestConsensusPolicy_ExecuteMajority(t *testing.T) {
	t.Parallel()
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"same"}`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"same"}`))
	}))
	defer b.Close()
	c := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":"different"}`))
	}))
	defer c.Close()

	cp := &ConsensusPolicy{MaxParticipants: 3, AgreementThreshold: 2}
	ups := []*upstream.Upstream{
		mkConsensusUpstream("a", a.URL),
		mkConsensusUpstream("b", b.URL),
		mkConsensusUpstream("c", c.URL),
	}

	resp, picked, err := cp.Execute(context.Background(), ups, func(u *upstream.Upstream) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, u.URL, nil)
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if picked == nil {
		t.Fatal("expected a picked upstream")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	if !strings.Contains(string(body), "same") {
		t.Fatalf("expected majority response body, got %q", string(body))
	}
}

func TestConsensusPolicy_ExecuteNoConsensus(t *testing.T) {
	t.Parallel()
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`a`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`b`))
	}))
	defer b.Close()
	c := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`c`))
	}))
	defer c.Close()

	cp := &ConsensusPolicy{MaxParticipants: 3, AgreementThreshold: 2}
	ups := []*upstream.Upstream{
		mkConsensusUpstream("a", a.URL),
		mkConsensusUpstream("b", b.URL),
		mkConsensusUpstream("c", c.URL),
	}

	_, _, err := cp.Execute(context.Background(), ups, func(u *upstream.Upstream) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, u.URL, nil)
	})
	if err == nil {
		t.Fatal("expected consensus error")
	}
}

func TestNetwork_ConsensusWithGzipUpstreams(t *testing.T) {
	payload := `{"data":{"root":"0xabc"}}`
	gzHandler := func(level int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				w.Header().Set("Content-Encoding", "gzip")
				gz, _ := gzip.NewWriterLevel(w, level)
				_, _ = gz.Write([]byte(payload))
				_ = gz.Close()
				return
			}
			_, _ = w.Write([]byte(payload))
		}
	}
	// Different compression levels emulate per-node nondeterministic gzip
	// output for identical JSON.
	a := httptest.NewServer(gzHandler(gzip.BestSpeed))
	defer a.Close()
	b := httptest.NewServer(gzHandler(gzip.BestCompression))
	defer b.Close()

	id := netID(t)
	cfg := mustCfg(t, id, a.URL, nil)
	cfg.Networks[0].Upstreams = []config.UpstreamConfig{
		{ID: "u1", URL: a.URL},
		{ID: "u2", URL: b.URL},
	}
	cfg.Failsafe.Consensus = &config.ConsensusConfig{
		Enabled:            true,
		MaxParticipants:    2,
		AgreementThreshold: 2,
	}

	n, err := New(&cfg.Networks[0], cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blocks/head/root", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	n.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q (gzip variance must not break consensus)", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Encoding") == "gzip" {
		t.Fatalf("small consensus body should not be gzip re-encoded")
	}
	if got := rec.Body.String(); got != payload {
		t.Fatalf("body: got %q want %q", got, payload)
	}
}
