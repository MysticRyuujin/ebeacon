package debuglog

import (
	"strings"
	"testing"
)

func TestSanitizeQuery_RedactsSecrets(t *testing.T) {
	t.Parallel()
	out := sanitizeQuery("secret=topsecret&x=1")
	if strings.Contains(out, "topsecret") {
		t.Fatalf("secret leaked: %q", out)
	}
}

func TestSanitizeQuery_MalformedQueryNeverLeaksRaw(t *testing.T) {
	t.Parallel()
	out := sanitizeQuery("secret=topsecret&x=%zz")
	if strings.Contains(out, "topsecret") {
		t.Fatalf("malformed query leaked the raw secret: %q", out)
	}
}
