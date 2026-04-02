package network

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/ebeacon/ebeacon/config"
	"github.com/ebeacon/ebeacon/upstream"
)

// compiledRouting holds compiled client routes and ordered route rules.
type compiledRouting struct {
	clientRoutes []compiledClientRoute
	routeRules   []compiledRouteRule
}

type compiledClientRoute struct {
	prefix     string // normalized, e.g. /lighthouse/
	upstreamID string
}

type compiledRouteRule struct {
	re         *regexp.Regexp
	methods    map[string]bool // uppercase; nil means any method
	upstreamID string
	deny       bool
}

func compileRouting(cfg *config.RoutingConfig) (*compiledRouting, error) {
	if len(cfg.ClientRoutes) == 0 && len(cfg.RouteRules) == 0 {
		return nil, nil
	}
	out := &compiledRouting{}
	for _, cr := range cfg.ClientRoutes {
		prefix := normalizeClientPathPrefix(cr.PathPrefix)
		if prefix == "" {
			return nil, fmt.Errorf("client route has empty pathPrefix after normalization")
		}
		out.clientRoutes = append(out.clientRoutes, compiledClientRoute{
			prefix:     prefix,
			upstreamID: cr.UpstreamID,
		})
	}
	for _, rr := range cfg.RouteRules {
		re, err := regexp.Compile(rr.PathPattern)
		if err != nil {
			return nil, fmt.Errorf("route rule pattern %q: %w", rr.PathPattern, err)
		}
		var methods map[string]bool
		if len(rr.Methods) > 0 {
			methods = make(map[string]bool, len(rr.Methods))
			for _, m := range rr.Methods {
				methods[strings.ToUpper(strings.TrimSpace(m))] = true
			}
		}
		out.routeRules = append(out.routeRules, compiledRouteRule{
			re:         re,
			methods:    methods,
			upstreamID: rr.UpstreamID,
			deny:       rr.Deny,
		})
	}
	return out, nil
}

func normalizeClientPathPrefix(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p
}

// applyClientPrefix implements dugtrio-style routing: /{client}/eth/v* → /eth/v* on the upstream.
// Non-standard paths keep the full URL (e.g. /lighthouse/custom).
func (r *compiledRouting) applyClientPrefix(path string) (newPath string, upstreamID string) {
	if r == nil || len(r.clientRoutes) == 0 {
		return path, ""
	}
	for _, cr := range r.clientRoutes {
		base := strings.TrimSuffix(cr.prefix, "/") // e.g. /lighthouse
		if base == "" {
			continue
		}
		if path != base && !strings.HasPrefix(path, cr.prefix) {
			continue
		}

		var rest string
		switch {
		case strings.HasPrefix(path, cr.prefix):
			rest = strings.TrimPrefix(path, cr.prefix)
		case path == base:
			rest = ""
		default:
			rest = ""
		}
		if rest != "" && !strings.HasPrefix(rest, "/") {
			rest = "/" + rest
		}
		if rest == "" {
			rest = "/"
		}

		trim := strings.TrimPrefix(rest, "/")
		if strings.HasPrefix(trim, "eth/v") || trim == "healthz" {
			return "/" + trim, cr.upstreamID
		}
		return path, cr.upstreamID
	}
	return path, ""
}

// matchRouteRule returns the first matching ordered rule (deny or upstream pin).
func (r *compiledRouting) matchRouteRule(method, path string) (deny bool, upstreamID string, matched bool) {
	if r == nil || len(r.routeRules) == 0 {
		return false, "", false
	}
	m := strings.ToUpper(method)
	for _, rule := range r.routeRules {
		if !rule.re.MatchString(path) {
			continue
		}
		if len(rule.methods) > 0 && !rule.methods[m] {
			continue
		}
		if rule.deny {
			return true, "", true
		}
		return false, rule.upstreamID, true
	}
	return false, "", false
}

func cloneRequestWithPath(r *http.Request, path string) *http.Request {
	u2 := *r.URL
	u2.Path = path
	u2.RawPath = ""
	r2 := new(http.Request)
	*r2 = *r
	r2.URL = &u2
	return r2
}

func isKnownClientType(selector string) bool {
	switch strings.ToLower(selector) {
	case upstream.ClientLighthouse,
		upstream.ClientPrysm,
		upstream.ClientTeku,
		upstream.ClientNimbus,
		upstream.ClientLodestar,
		upstream.ClientGrandine,
		upstream.ClientCaplin:
		return true
	default:
		return false
	}
}

func inferClientSelectorPath(path string, pool interface {
	ByID(id string) *upstream.Upstream
	SelectByClientType(clientType string, n int) []*upstream.Upstream
}) (newPath string, upstreamID string, matched bool) {
	trimmed := strings.TrimPrefix(path, "/")
	selector, rest, ok := strings.Cut(trimmed, "/")
	if !ok || selector == "" {
		return path, "", false
	}
	if !strings.HasPrefix(rest, "eth/v") && rest != "healthz" {
		return path, "", false
	}
	rewritten := "/" + rest
	if isKnownClientType(selector) {
		clientType := strings.ToLower(selector)
		if len(pool.SelectByClientType(clientType, 1)) > 0 {
			return rewritten, "client:" + clientType, true
		}
	}
	if pool.ByID(selector) != nil {
		return rewritten, selector, true
	}
	return path, "", false
}

func (n *Network) rewriteClientPath(r *http.Request) (*http.Request, string) {
	if n.routing != nil {
		newPath, clientUpstream := n.routing.applyClientPrefix(r.URL.Path)
		if clientUpstream != "" {
			if newPath != r.URL.Path {
				return cloneRequestWithPath(r, newPath), clientUpstream
			}
			return r, clientUpstream
		}
	}
	newPath, clientUpstream, matched := inferClientSelectorPath(r.URL.Path, n.pool)
	if matched {
		if newPath != r.URL.Path {
			return cloneRequestWithPath(r, newPath), clientUpstream
		}
		return r, clientUpstream
	}
	return r, ""
}
