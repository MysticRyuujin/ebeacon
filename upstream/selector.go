package upstream

import (
	"path"
	"strings"
)

// Selector restricts routing to upstreams by exact ID, ID glob pattern, or
// client type. The zero Selector matches every upstream.
type Selector struct {
	ID         string
	ClientType string
	Glob       string
}

// ParseSelector parses "client:<type>", "glob:<pattern>", a bare glob pattern,
// or an upstream ID. "glob:" is String's spelling of a glob selector in
// cache-key scopes. An upstream literally named "glob:x" would misparse,
// which is acceptable because upstream IDs cannot contain ':'.
func ParseSelector(s string) Selector {
	switch {
	case strings.HasPrefix(s, "client:"):
		return Selector{ClientType: strings.TrimPrefix(s, "client:")}
	case strings.HasPrefix(s, "glob:"):
		return Selector{Glob: strings.TrimPrefix(s, "glob:")}
	case strings.ContainsAny(s, "*?["):
		return Selector{Glob: s}
	default:
		return Selector{ID: s}
	}
}

// Enabled reports whether s restricts the upstream set.
func (s Selector) Enabled() bool {
	return s.ID != "" || s.ClientType != "" || s.Glob != ""
}

// String returns s in ParseSelector syntax.
func (s Selector) String() string {
	switch {
	case s.ClientType != "":
		return "client:" + s.ClientType
	case s.Glob != "":
		return "glob:" + s.Glob
	default:
		return s.ID
	}
}

// Match reports whether u satisfies s.
func (s Selector) Match(u *Upstream) bool {
	switch {
	case s.ClientType != "":
		return u.ClientType() == s.ClientType
	case s.Glob != "":
		matched, _ := path.Match(s.Glob, u.ID)
		return matched
	case s.ID != "":
		return u.ID == s.ID
	default:
		return true
	}
}
