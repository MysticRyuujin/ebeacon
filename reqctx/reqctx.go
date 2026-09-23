package reqctx

import (
	"context"
	"net/http"
)

type contextKey int

const apiKeyIDKey contextKey = 0

// WithAPIKeyID attaches the authenticated API key ID to the request context.
func WithAPIKeyID(r *http.Request, keyID string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), apiKeyIDKey, keyID))
}

// APIKeyIDFromRequest extracts the authenticated API key ID from the request context.
// Returns empty string if no key was set.
func APIKeyIDFromRequest(r *http.Request) string {
	if v, ok := r.Context().Value(apiKeyIDKey).(string); ok {
		return v
	}
	return ""
}
