package proxy

import (
	"net/http"

	"github.com/mysticryuujin/ebeacon/reqctx"
)

// WithAPIKeyID attaches the authenticated API key ID to the request context.
func WithAPIKeyID(r *http.Request, keyID string) *http.Request {
	return reqctx.WithAPIKeyID(r, keyID)
}

// APIKeyIDFromRequest extracts the authenticated API key ID from the request context.
func APIKeyIDFromRequest(r *http.Request) string {
	return reqctx.APIKeyIDFromRequest(r)
}

// WithAPIKeyTier attaches the authenticated API key tier to the request context.
func WithAPIKeyTier(r *http.Request, tier string) *http.Request {
	return reqctx.WithAPIKeyTier(r, tier)
}

// APIKeyTierFromRequest extracts the API key tier from the request context.
func APIKeyTierFromRequest(r *http.Request) string {
	return reqctx.APIKeyTierFromRequest(r)
}
