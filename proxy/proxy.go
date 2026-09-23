// Package proxy routes incoming HTTP requests to the correct network handler
// based on the URL path prefix: /{networkId}/eth/v1/...
package proxy

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/mysticryuujin/ebeacon/config"
	networkpkg "github.com/mysticryuujin/ebeacon/network"
	"github.com/mysticryuujin/ebeacon/reqctx"
	"github.com/mysticryuujin/ebeacon/upstream"
	"golang.org/x/time/rate"
)

// Proxy is the top-level HTTP handler. It extracts the network prefix from
// the request path and delegates to the appropriate network handler.
type Proxy struct {
	networks map[string]*networkpkg.Network
	auth     *config.AuthConfig

	keyLimiters  map[string]*rate.Limiter
	tierLimiters map[string]*rate.Limiter

	// single is set when exactly one network is configured, enabling
	// prefix-free requests: /eth/v1/... works in addition to /mainnet/eth/v1/...
	single *networkpkg.Network
}

// New creates a Proxy from a map of network handlers.
func New(networks map[string]*networkpkg.Network) *Proxy {
	p := &Proxy{networks: networks}
	if len(networks) == 1 {
		for _, n := range networks {
			p.single = n
		}
	}
	return p
}

// SetAuth configures authentication for top-level network routes.
func (p *Proxy) SetAuth(auth *config.AuthConfig) {
	p.auth = auth
	p.keyLimiters, p.tierLimiters = buildAuthRateLimiters(auth)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		p.serveHealthz(w)
		return
	}

	networkID, rest, pathKey, strip, ok := p.routeRequest(r)
	if !ok {
		r2, allowed := p.authenticate(w, r, "")
		if !allowed {
			return
		}
		// Single-network mode: route the original path unmodified.
		if p.single != nil {
			p.single.ServeHTTP(w, r2)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/")
		networkID = path
		if i := strings.IndexByte(path, '/'); i >= 0 {
			networkID = path[:i]
		}
		http.Error(w, fmt.Sprintf("unknown network %q — available: %s",
			networkID, p.availableNetworks()), http.StatusNotFound)
		return
	}

	r2, allowed := p.authenticate(w, r, pathKey)
	if !allowed {
		return
	}
	n := p.networks[networkID]

	// Rewrite the request to strip the network prefix.
	// Shallow-copy the request struct and swap only the URL; cloning headers is
	// unnecessary since nothing downstream writes to r.Header, and avoiding it
	// saves ~1 GB of allocations under load.
	u2 := *r2.URL
	u2.Path = rest
	// Preserve percent-encoded segment content by stripping the same number
	// of leading segments from the escaped path. net/url ignores RawPath
	// unless it is a valid encoding of Path, so a mismatch degrades safely.
	u2.RawPath = ""
	if esc := r2.URL.EscapedPath(); esc != rest {
		if er := stripPathSegments(esc, strip); er != rest {
			u2.RawPath = er
		}
	}
	r3 := new(http.Request)
	*r3 = *r2
	r3.URL = &u2

	n.ServeHTTP(w, r3)
}

func stripPathSegments(path string, n int) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if n >= len(segments) {
		return "/"
	}
	return "/" + strings.Join(segments[n:], "/")
}

func (p *Proxy) authenticate(w http.ResponseWriter, r *http.Request, pathKey string) (*http.Request, bool) {
	if p.auth == nil {
		return r, true
	}
	result := AuthenticateRequest(r, p.auth, pathKey)
	if !result.Authenticated {
		w.Header().Set("WWW-Authenticate", `Bearer realm="ebeacon"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	if result.KeyID != "" {
		r = reqctx.WithAPIKeyID(r, result.KeyID)
	}
	if !applyAuthRateLimits(w, p.keyLimiters, p.tierLimiters, result) {
		return nil, false
	}
	return r, true
}

func (p *Proxy) routeRequest(r *http.Request) (networkID, rest, pathKey string, strip int, ok bool) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		return "", "", "", 0, false
	}
	segments := strings.Split(path, "/")
	if _, exists := p.networks[segments[0]]; exists {
		// Support /{network}/{api-key}/... in addition to /{network}/...
		if len(segments) >= 2 && p.isPathKey(segments[1]) {
			return segments[0], joinRest(segments[2:]), segments[1], 2, true
		}
		return segments[0], joinRest(segments[1:]), "", 1, true
	}
	if p.auth != nil && len(segments) >= 2 {
		if _, exists := p.networks[segments[1]]; exists {
			return segments[1], joinRest(segments[2:]), segments[0], 2, true
		}
	}
	return "", "", "", 0, false
}

// isPathKey reports whether candidate matches a configured secret, used to
// detect /{network}/{api-key}/... style URLs during routing.
func (p *Proxy) isPathKey(candidate string) bool {
	if p.auth == nil || candidate == "" {
		return false
	}
	for _, key := range p.auth.Keys {
		if key.Secret != "" && constantTimeEquals(candidate, key.Secret) {
			return true
		}
	}
	return p.auth.Secret != "" && constantTimeEquals(candidate, p.auth.Secret)
}

func (p *Proxy) availableNetworks() string {
	return strings.Join(slices.Sorted(maps.Keys(p.networks)), ", ")
}

func (p *Proxy) serveHealthz(w http.ResponseWriter) {
	// Overall status: ok if all networks are healthy, degraded if any is
	// degraded, and down only if all are down.
	networks := make(map[string]string, len(p.networks))
	best := upstream.HealthDown
	for id, n := range p.networks {
		hs := n.HealthStatus()
		networks[id], _ = networkpkg.HealthzStatus(hs)
		if hs == upstream.HealthUp && best == upstream.HealthDown {
			best = upstream.HealthUp
		} else if hs == upstream.HealthDegraded {
			best = upstream.HealthDegraded
		}
	}
	status, code := networkpkg.HealthzStatus(best)

	body, _ := json.Marshal(struct {
		Status   string            `json:"status"`
		Networks map[string]string `json:"networks"`
	}{status, networks})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(body) //nolint:errcheck
}

func joinRest(segments []string) string {
	if len(segments) == 0 {
		return "/"
	}
	return "/" + strings.Join(segments, "/")
}
