package network

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ebeacon/ebeacon/config"
	"github.com/ebeacon/ebeacon/upstream"
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
