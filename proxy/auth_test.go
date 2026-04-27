package proxy

import (
	"net/http"
	"testing"

	"github.com/mysticryuujin/ebeacon/config"
)

func TestAuthSecretMatches(t *testing.T) {
	t.Parallel()
	secret := "abc123"
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	if !AuthSecretMatches(req, "") {
		t.Fatal("empty secret should allow")
	}
	req.Header.Set(headerEbeaconSecretToken, secret)
	if !AuthSecretMatches(req, secret) {
		t.Fatal("X-EBEACON-Secret-Token")
	}
	req2, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req2.Header.Set("Authorization", "Bearer "+secret)
	if !AuthSecretMatches(req2, secret) {
		t.Fatal("Bearer")
	}
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost/?secret="+secret, nil)
	if !AuthSecretMatches(req3, secret) {
		t.Fatal("query secret")
	}
}

func TestAuthenticateRequest_NilAuth(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	result := AuthenticateRequest(req, nil)
	if !result.Authenticated {
		t.Fatal("nil auth should allow all requests")
	}
	if result.KeyID != "" {
		t.Fatalf("expected empty KeyID, got %q", result.KeyID)
	}
}

func TestAuthenticateRequest_EmptyAuth(t *testing.T) {
	t.Parallel()
	auth := &config.AuthConfig{}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	result := AuthenticateRequest(req, auth)
	if !result.Authenticated {
		t.Fatal("empty auth should allow all requests")
	}
}

func TestAuthenticateRequest_SingleSecret(t *testing.T) {
	t.Parallel()
	auth := &config.AuthConfig{Secret: "mysecret"}

	// No credentials → denied
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	result := AuthenticateRequest(req, auth)
	if result.Authenticated {
		t.Fatal("expected denial without credentials")
	}

	// Valid via header
	req2, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req2.Header.Set("Authorization", "Bearer mysecret")
	result2 := AuthenticateRequest(req2, auth)
	if !result2.Authenticated {
		t.Fatal("expected auth with Bearer header")
	}
	if result2.KeyID != "default" {
		t.Fatalf("expected KeyID=default, got %q", result2.KeyID)
	}

	// Wrong secret
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req3.Header.Set("Authorization", "Bearer wrong")
	result3 := AuthenticateRequest(req3, auth)
	if result3.Authenticated {
		t.Fatal("expected denial with wrong secret")
	}
}

func TestAuthenticateRequest_NamedKeys(t *testing.T) {
	t.Parallel()
	auth := &config.AuthConfig{
		Keys: []config.APIKeyConfig{
			{ID: "validator", Secret: "val-key-123"},
			{ID: "monitoring", Secret: "mon-key-456"},
		},
	}

	// No credentials → denied
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	result := AuthenticateRequest(req, auth)
	if result.Authenticated {
		t.Fatal("expected denial without credentials")
	}

	// Match first key via X-Ebeacon-Secret-Token
	req2, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req2.Header.Set(headerEbeaconSecretToken, "val-key-123")
	result2 := AuthenticateRequest(req2, auth)
	if !result2.Authenticated {
		t.Fatal("expected auth with validator key")
	}
	if result2.KeyID != "validator" {
		t.Fatalf("expected KeyID=validator, got %q", result2.KeyID)
	}

	// Match second key via query string
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost/?secret=mon-key-456", nil)
	result3 := AuthenticateRequest(req3, auth)
	if !result3.Authenticated {
		t.Fatal("expected auth with monitoring key")
	}
	if result3.KeyID != "monitoring" {
		t.Fatalf("expected KeyID=monitoring, got %q", result3.KeyID)
	}

	// Wrong secret
	req4, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req4.Header.Set("Authorization", "Bearer wrong-key")
	result4 := AuthenticateRequest(req4, auth)
	if result4.Authenticated {
		t.Fatal("expected denial with unknown key")
	}
}

func TestAuthenticateRequest_NamedKeysWithFallbackSecret(t *testing.T) {
	t.Parallel()
	auth := &config.AuthConfig{
		Secret: "legacy-secret",
		Keys: []config.APIKeyConfig{
			{ID: "app", Secret: "app-key"},
		},
	}

	// Named key works
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req.Header.Set("Authorization", "Bearer app-key")
	result := AuthenticateRequest(req, auth)
	if !result.Authenticated || result.KeyID != "app" {
		t.Fatal("expected named key match")
	}

	// Legacy secret also works
	req2, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req2.Header.Set("Authorization", "Bearer legacy-secret")
	result2 := AuthenticateRequest(req2, auth)
	if !result2.Authenticated || result2.KeyID != "default" {
		t.Fatal("expected fallback to legacy secret")
	}
}

func TestExtractSecret_AllSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(r *http.Request)
		want  string
	}{
		{
			name:  "X-EBEACON-Secret-Token",
			setup: func(r *http.Request) { r.Header.Set(headerEbeaconSecretToken, "s1") },
			want:  "s1",
		},
		{
			name:  "X-API-Key",
			setup: func(r *http.Request) { r.Header.Set(headerAPIKey, "s2") },
			want:  "s2",
		},
		{
			name:  "Bearer token",
			setup: func(r *http.Request) { r.Header.Set("Authorization", "Bearer s3") },
			want:  "s3",
		},
		{
			name:  "Query parameter",
			setup: func(r *http.Request) {},
			want:  "s4",
		},
		{
			name:  "No secret",
			setup: func(r *http.Request) {},
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "http://localhost/"
			if tt.name == "Query parameter" {
				url = "http://localhost/?secret=s4"
			}
			req, _ := http.NewRequest(http.MethodGet, url, nil)
			tt.setup(req)
			got := extractSecret(req)
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestExtractSecret_NewAPIKeyHeaders(t *testing.T) {
	t.Parallel()

	// X-API-Key
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req.Header.Set(headerAPIKey, "key-via-x-api-key")
	if got := extractSecret(req); got != "key-via-x-api-key" {
		t.Fatalf("X-API-Key: got %q", got)
	}
}

func TestAuthenticateRequest_Tiers(t *testing.T) {
	t.Parallel()
	auth := &config.AuthConfig{
		Keys: []config.APIKeyConfig{
			{ID: "free-user", Secret: "free-secret", Tier: "free"},
			{ID: "premium-user", Secret: "premium-secret", Tier: "premium"},
			{ID: "no-tier-user", Secret: "no-tier-secret"},
		},
		Tiers: []config.TierConfig{
			{Name: "free", Description: "Free tier"},
			{Name: "premium", Description: "Premium tier"},
		},
	}

	tests := []struct {
		name     string
		secret   string
		wantKey  string
		wantTier string
	}{
		{"free key", "free-secret", "free-user", "free"},
		{"premium key", "premium-secret", "premium-user", "premium"},
		{"no-tier key", "no-tier-secret", "no-tier-user", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
			req.Header.Set(headerAPIKey, tt.secret)
			result := AuthenticateRequest(req, auth)
			if !result.Authenticated {
				t.Fatal("expected authentication to succeed")
			}
			if result.KeyID != tt.wantKey {
				t.Fatalf("KeyID: got %q, want %q", result.KeyID, tt.wantKey)
			}
			if result.Tier != tt.wantTier {
				t.Fatalf("Tier: got %q, want %q", result.Tier, tt.wantTier)
			}
		})
	}
}

func TestAuthenticateRequest_PathKey(t *testing.T) {
	t.Parallel()
	auth := &config.AuthConfig{
		Keys: []config.APIKeyConfig{
			{ID: "path-key-user", Secret: "path-secret-abc"},
		},
	}

	// Key presented via extraSecrets (path extraction) — no header set.
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	result := AuthenticateRequest(req, auth, "path-secret-abc")
	if !result.Authenticated {
		t.Fatal("expected auth via path key")
	}
	if result.KeyID != "path-key-user" {
		t.Fatalf("KeyID: got %q, want %q", result.KeyID, "path-key-user")
	}

	// Wrong path key → denied.
	result2 := AuthenticateRequest(req, auth, "wrong-path-secret")
	if result2.Authenticated {
		t.Fatal("expected denial with wrong path key")
	}

	// Header takes precedence over path key when both present.
	req3, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	req3.Header.Set(headerAPIKey, "path-secret-abc")
	result3 := AuthenticateRequest(req3, auth, "wrong-path-secret")
	if !result3.Authenticated {
		t.Fatal("expected header to succeed even when path key is wrong")
	}
}
