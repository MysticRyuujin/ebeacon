// Package proxy routes incoming HTTP requests to the correct network handler
// based on the URL path prefix: /{networkId}/eth/v1/...
package proxy

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/ebeacon/ebeacon/config"
	networkpkg "github.com/ebeacon/ebeacon/network"
	"github.com/ebeacon/ebeacon/upstream"
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

	networkID, rest, pathKey, ok := p.routeRequest(r)
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
	u2.RawPath = ""
	r3 := new(http.Request)
	*r3 = *r2
	r3.URL = &u2

	n.ServeHTTP(w, r3)
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
		r = WithAPIKeyID(r, result.KeyID)
	}
	if result.Tier != "" {
		r = WithAPIKeyTier(r, result.Tier)
	}
	if !applyAuthRateLimits(w, p.keyLimiters, p.tierLimiters, result) {
		return nil, false
	}
	return r, true
}

func (p *Proxy) routeRequest(r *http.Request) (networkID, rest, pathKey string, ok bool) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		return "", "", "", false
	}
	segments := strings.Split(path, "/")
	if _, exists := p.networks[segments[0]]; exists {
		return segments[0], joinRest(segments[1:]), "", true
	}
	if p.auth != nil && len(segments) >= 2 {
		if _, exists := p.networks[segments[1]]; exists {
			return segments[1], joinRest(segments[2:]), segments[0], true
		}
	}
	return "", "", "", false
}

func (p *Proxy) availableNetworks() string {
	ids := make([]string, 0, len(p.networks))
	for id := range p.networks {
		ids = append(ids, id)
	}
	return strings.Join(ids, ", ")
}

func (p *Proxy) serveHealthz(w http.ResponseWriter) {
	// Determine overall status: ok if all networks healthy, degraded if any
	// are degraded, down only if all are down.
	allDown := true
	anyDegraded := false
	ids := make([]string, 0, len(p.networks))
	for id := range p.networks {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		switch p.networks[id].HealthStatus() {
		case upstream.HealthUp:
			allDown = false
		case upstream.HealthDegraded:
			allDown = false
			anyDegraded = true
		}
	}

	var status string
	var code int
	switch {
	case allDown:
		status, code = "down", http.StatusServiceUnavailable
	case anyDegraded:
		status, code = "degraded", http.StatusOK
	default:
		status, code = "ok", http.StatusOK
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"status":%q,"networks":{`, status) //nolint:errcheck
	for i, id := range ids {
		ns := p.networks[id].HealthStatus()
		var ns2 string
		switch ns {
		case upstream.HealthUp:
			ns2 = "ok"
		case upstream.HealthDegraded:
			ns2 = "degraded"
		default:
			ns2 = "down"
		}
		if i > 0 {
			fmt.Fprint(w, ",") //nolint:errcheck
		}
		fmt.Fprintf(w, "%q:%q", id, ns2) //nolint:errcheck
	}
	fmt.Fprint(w, "}}") //nolint:errcheck
}

func joinRest(segments []string) string {
	if len(segments) == 0 {
		return "/"
	}
	return "/" + strings.Join(segments, "/")
}
