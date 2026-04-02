package proxy

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/ebeacon/ebeacon/config"
)

// Supported authentication headers, in priority order.
const (
	headerEbeaconSecretToken = "X-EBEACON-Secret-Token"
	// Standard API key header — accepted for broad client compatibility.
	headerAPIKey = "X-API-Key"
)

// AuthResult is returned by AuthenticateRequest with the matched API key identity.
type AuthResult struct {
	Authenticated bool
	KeyID         string // API key name/id ("default" for unnamed single-secret configs)
	Tier          string // API key tier (empty if no tier assigned)
}

// AuthenticateRequest checks the request against the given AuthConfig and returns
// the matched API key identity. If auth is nil or has no secrets configured, access is allowed.
// extraSecrets is an optional list of additional credentials (e.g. extracted from the URL path);
// they are tried if no credential is found in the request headers/query.
func AuthenticateRequest(r *http.Request, auth *config.AuthConfig, extraSecrets ...string) AuthResult {
	if auth == nil {
		return AuthResult{Authenticated: true, KeyID: ""}
	}

	presented := extractSecret(r)
	if presented == "" && len(extraSecrets) > 0 {
		presented = extraSecrets[0]
	}

	// Named API keys take precedence.
	if len(auth.Keys) > 0 {
		for _, key := range auth.Keys {
			if key.Secret != "" && constantTimeEquals(presented, key.Secret) {
				return AuthResult{Authenticated: true, KeyID: key.ID, Tier: key.Tier}
			}
		}
		// Fall through to check the legacy secret field if it's also set.
	}

	// Backward compat: single unnamed secret.
	if auth.Secret == "" {
		// No secret configured at all (and no named keys matched).
		if len(auth.Keys) > 0 {
			return AuthResult{} // named keys configured but none matched
		}
		return AuthResult{Authenticated: true, KeyID: ""}
	}
	if constantTimeEquals(presented, auth.Secret) {
		return AuthResult{Authenticated: true, KeyID: "default"}
	}
	return AuthResult{}
}

// AuthSecretMatches checks whether the request presents the expected API key / secret.
// Deprecated: prefer AuthenticateRequest for new code.
func AuthSecretMatches(r *http.Request, secret string) bool {
	if secret == "" {
		return true
	}
	return constantTimeEquals(extractSecret(r), secret)
}

// extractSecret pulls the credential from the request using all supported mechanisms:
// X-EBEACON-Secret-Token, X-API-Key, Authorization: Bearer, ?secret=
func extractSecret(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get(headerEbeaconSecretToken)); v != "" {
		return v
	}
	if v := strings.TrimSpace(r.Header.Get(headerAPIKey)); v != "" {
		return v
	}
	if auth := r.Header.Get("Authorization"); auth != "" {
		auth = strings.TrimSpace(auth)
		if len(auth) > 6 && strings.EqualFold(auth[:6], "bearer") {
			if token := strings.TrimSpace(auth[6:]); token != "" {
				return token
			}
		}
	}
	if q := r.URL.Query().Get("secret"); q != "" {
		return q
	}
	return ""
}

// constantTimeEquals performs a constant-time comparison to prevent timing attacks.
func constantTimeEquals(a, b string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
