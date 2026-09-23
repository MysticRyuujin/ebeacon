package network

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/upstream"
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
	pathMethodRule
	upstreamID string
	deny       bool
}

// pathMethodRule matches a path regex and, when methods is non-nil, one of
// the uppercase methods.
type pathMethodRule struct {
	re      *regexp.Regexp
	methods map[string]bool
}

func compilePathMethodRule(pattern string, methods []string) (pathMethodRule, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return pathMethodRule{}, err
	}
	rule := pathMethodRule{re: re}
	if len(methods) > 0 {
		rule.methods = make(map[string]bool, len(methods))
		for _, m := range methods {
			rule.methods[strings.ToUpper(strings.TrimSpace(m))] = true
		}
	}
	return rule, nil
}

// matches reports whether the rule applies; method must be uppercase.
func (r pathMethodRule) matches(method, path string) bool {
	return (r.methods == nil || r.methods[method]) && r.re.MatchString(path)
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
		rule, err := compilePathMethodRule(rr.PathPattern, rr.Methods)
		if err != nil {
			return nil, fmt.Errorf("route rule pattern %q: %w", rr.PathPattern, err)
		}
		out.routeRules = append(out.routeRules, compiledRouteRule{
			pathMethodRule: rule,
			upstreamID:     rr.UpstreamID,
			deny:           rr.Deny,
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
		if strings.HasPrefix(path, cr.prefix) {
			rest = strings.TrimPrefix(path, cr.prefix)
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
		if !rule.matches(m, path) {
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

func inferClientSelectorPath(path string, pool interface {
	ByID(id string) *upstream.Upstream
	HasMatching(sel upstream.Selector) bool
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
	if upstream.IsKnownClientType(selector) {
		clientType := strings.ToLower(selector)
		if pool.HasMatching(upstream.Selector{ClientType: clientType}) {
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
