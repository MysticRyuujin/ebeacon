package upstream

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestSanitizeErrorRedactsCredentialBearingURL(t *testing.T) {
	original := &url.Error{
		Op:  "Get",
		URL: "https://user:password@beacon.example/api-key/eth/v1/node/health?token=secret#fragment",
		Err: errors.New("connection refused"),
	}

	got := SanitizeError(original)
	text := got.Error()
	for _, secret := range []string{"user", "password", "api-key", "token", "secret", "fragment"} {
		if strings.Contains(text, secret) {
			t.Fatalf("sanitized error leaked %q: %q", secret, text)
		}
	}
	if !strings.Contains(text, "https://beacon.example") || !strings.Contains(text, "connection refused") {
		t.Fatalf("sanitized error lost useful context: %q", text)
	}
	if original.URL == "https://beacon.example" {
		t.Fatal("SanitizeError mutated the original error")
	}
}

func TestSanitizeErrorLeavesNonURLErrorUnchanged(t *testing.T) {
	original := errors.New("plain error")
	if got := SanitizeError(original); got != original {
		t.Fatalf("non-URL error changed: %v", got)
	}
}
