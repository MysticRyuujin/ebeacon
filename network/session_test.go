package network

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientIP_TrustedProxies(t *testing.T) {
	mkReq := func(remoteAddr, xff, xri string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = remoteAddr
		if xff != "" {
			r.Header.Set("X-Forwarded-For", xff)
		}
		if xri != "" {
			r.Header.Set("X-Real-IP", xri)
		}
		return r
	}

	tests := []struct {
		name    string
		trusted []string
		req     *http.Request
		want    string
	}{
		{"legacy trusts xff", nil, mkReq("203.0.113.9:1234", "1.2.3.4", ""), "1.2.3.4"},
		{"legacy trusts xri", nil, mkReq("203.0.113.9:1234", "", "5.6.7.8"), "5.6.7.8"},
		{"legacy no headers", nil, mkReq("203.0.113.9:1234", "", ""), "203.0.113.9"},
		{"untrusted peer ignores spoofed xff", []string{"10.0.0.0/8"}, mkReq("203.0.113.9:1234", "1.2.3.4", ""), "203.0.113.9"},
		{"untrusted peer ignores xri", []string{"10.0.0.0/8"}, mkReq("203.0.113.9:1234", "", "5.6.7.8"), "203.0.113.9"},
		{"trusted peer takes client from xff", []string{"10.0.0.0/8"}, mkReq("10.0.0.5:1234", "1.2.3.4", ""), "1.2.3.4"},
		{"trusted peer walks past trusted hops", []string{"10.0.0.0/8"}, mkReq("10.0.0.5:1234", "1.2.3.4, 10.0.0.7", ""), "1.2.3.4"},
		{"spoofed prefix stops at first untrusted", []string{"10.0.0.0/8"}, mkReq("10.0.0.5:1234", "9.9.9.9, 1.2.3.4, 10.0.0.7", ""), "1.2.3.4"},
		{"all trusted chain returns leftmost", []string{"10.0.0.0/8"}, mkReq("10.0.0.5:1234", "10.0.0.1, 10.0.0.2", ""), "10.0.0.1"},
		{"trusted peer honors xri without xff", []string{"10.0.0.0/8"}, mkReq("10.0.0.5:1234", "", "5.6.7.8"), "5.6.7.8"},
		{"trusted peer no headers", []string{"10.0.0.0/8"}, mkReq("10.0.0.5:1234", "", ""), "10.0.0.5"},
		{"bare ip trust entry", []string{"127.0.0.1"}, mkReq("127.0.0.1:9999", "1.2.3.4", ""), "1.2.3.4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := SetTrustedProxies(tt.trusted); err != nil {
				t.Fatal(err)
			}
			defer SetTrustedProxies(nil) //nolint:errcheck
			if got := ClientIP(tt.req); got != tt.want {
				t.Fatalf("ClientIP = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSetTrustedProxies_RejectsGarbage(t *testing.T) {
	if err := SetTrustedProxies([]string{"not-an-ip"}); err == nil {
		t.Fatal("expected error for invalid trusted proxy entry")
	}
	SetTrustedProxies(nil) //nolint:errcheck
}
