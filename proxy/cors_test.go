package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ebeacon/ebeacon/config"
)

func TestWrapWithCORS_AllowedOrigin(t *testing.T) {
	t.Parallel()

	hits := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("X-Ebeacon-Cache", "HIT")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	})

	allowCredentials := false
	handler := WrapWithCORS(next, &config.CORSConfig{
		AllowedOrigins:   []string{"https://app.example.com"},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"content-type", "authorization"},
		ExposedHeaders:   []string{"X-Ebeacon-Cache"},
		AllowCredentials: &allowCredentials,
		MaxAge:           600,
	})

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if hits != 1 {
		t.Fatalf("expected next handler to be called once, got %d", hits)
	}
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status: got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("allow origin: got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "X-Ebeacon-Cache" {
		t.Fatalf("expose headers: got %q", got)
	}
	if !hasVaryValue(rec.Header(), "Origin") {
		t.Fatal("expected Vary: Origin")
	}
	if got := rec.Header().Get("X-Ebeacon-Cache"); got != "HIT" {
		t.Fatalf("expected downstream headers to be preserved, got %q", got)
	}
}

func TestWrapWithCORS_PreflightHandledLocally(t *testing.T) {
	t.Parallel()

	hits := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusTeapot)
	})

	allowCredentials := false
	handler := WrapWithCORS(next, &config.CORSConfig{
		AllowedOrigins:   []string{"https://app.example.com"},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"content-type", "x-api-key"},
		AllowCredentials: &allowCredentials,
		MaxAge:           3600,
	})

	req := httptest.NewRequest(http.MethodOptions, "/mainnet/eth/v1/beacon/headers", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "x-api-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if hits != 0 {
		t.Fatalf("expected preflight to bypass next handler, got %d hits", hits)
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status: got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("allow origin: got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET, POST, OPTIONS" {
		t.Fatalf("allow methods: got %q", got)
	}
	if !hasVaryValue(rec.Header(), "Access-Control-Request-Method") {
		t.Fatal("expected Vary: Access-Control-Request-Method")
	}
	if !hasVaryValue(rec.Header(), "Access-Control-Request-Headers") {
		t.Fatal("expected Vary: Access-Control-Request-Headers")
	}
}

func TestWrapWithCORS_DisallowedOriginContinuesWithoutHeaders(t *testing.T) {
	t.Parallel()

	hits := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	})

	allowCredentials := false
	handler := WrapWithCORS(next, &config.CORSConfig{
		AllowedOrigins:   []string{"https://app.example.com"},
		AllowedMethods:   []string{"GET", "OPTIONS"},
		AllowedHeaders:   []string{"content-type"},
		AllowCredentials: &allowCredentials,
	})

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if hits != 1 {
		t.Fatalf("expected next handler to be called once, got %d", hits)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("unexpected allow origin header %q", got)
	}
}

func TestWrapWithCORS_WildcardOriginWithCredentialsEchoesOrigin(t *testing.T) {
	t.Parallel()

	allowCredentials := true
	handler := WrapWithCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), &config.CORSConfig{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "OPTIONS"},
		AllowedHeaders:   []string{"authorization"},
		AllowCredentials: &allowCredentials,
	})

	req := httptest.NewRequest(http.MethodGet, "/eth/v1/node/version", nil)
	req.Header.Set("Origin", "https://wallet.example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://wallet.example.com" {
		t.Fatalf("allow origin: got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Fatalf("allow credentials: got %q", got)
	}
}

func hasVaryValue(headers http.Header, want string) bool {
	for _, value := range headers.Values("Vary") {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), want) {
				return true
			}
		}
	}
	return false
}
