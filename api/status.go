package api

import (
	"cmp"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/debuglog"
	networkpkg "github.com/mysticryuujin/ebeacon/network"
	"github.com/mysticryuujin/ebeacon/proxy"
	"github.com/mysticryuujin/ebeacon/upstream"
)

// StatusAPI serves JSON status endpoints for the web UI.
type StatusAPI struct {
	Networks map[string]*networkpkg.Network
	Auth     *config.AuthConfig // required when the UI is enabled; requests are denied when unset
}

// NewStatusAPI creates a StatusAPI.
func NewStatusAPI(networks map[string]*networkpkg.Network) *StatusAPI {
	return &StatusAPI{Networks: networks}
}

// RegisterRoutes registers all API routes on the given mux under the given basePath.
func (s *StatusAPI) RegisterRoutes(mux *http.ServeMux, basePath string) {
	basePath = strings.TrimRight(basePath, "/")
	mux.HandleFunc(basePath+"/", s.serve(basePath))
}

func (s *StatusAPI) serve(basePath string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		target, pathToken, ok := parseStatusRoute(basePath, r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		if !s.authenticate(w, r, pathToken) {
			return
		}
		switch target {
		case "", "/":
			s.handleUI(w, r)
		case "api/health":
			s.handleHealth(w, r)
		case "api/upstreams":
			s.handleUpstreams(w, r)
		case "api/cache":
			s.handleCache(w, r)
		case "api/cache/entries":
			s.handleCacheEntries(w, r)
		case "api/sessions":
			s.handleSessions(w, r)
		case "api/forks":
			s.handleForks(w, r)
		default:
			http.NotFound(w, r)
		}
	}
}

func (s *StatusAPI) authenticate(w http.ResponseWriter, r *http.Request, pathToken string) bool {
	if s.Auth == nil || !proxy.AuthenticateRequest(r, s.Auth, pathToken).Authenticated {
		w.Header().Set("WWW-Authenticate", `Bearer realm="ebeacon-dashboard"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func parseStatusRoute(basePath, path string) (target, pathToken string, ok bool) {
	prefix := basePath + "/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	rel := strings.TrimPrefix(path, prefix)
	if rel == "" {
		return "", "", true
	}
	if rel == "api" || strings.HasPrefix(rel, "api/") {
		return rel, "", true
	}
	pathToken, rest, _ := strings.Cut(rel, "/")
	if rest == "" {
		return "", pathToken, true
	}
	return rest, pathToken, true
}

type upstreamStatus struct {
	ID                string  `json:"id"`
	ObfuscatedID      string  `json:"obfuscatedId"`
	Network           string  `json:"network"`
	URL               string  `json:"url"`
	LoadBalancing     string  `json:"loadBalancing"`
	Health            string  `json:"health"`
	HeadSlot          uint64  `json:"headSlot"`
	HeadRoot          string  `json:"headRoot"`
	SyncDistance      uint64  `json:"syncDistance"`
	ClientType        string  `json:"clientType"`
	ActiveConn        int64   `json:"activeConnections"`
	Priority          int     `json:"priority"`
	Weight            int     `json:"weight"`
	Archive           bool    `json:"archive"`
	Score             float64 `json:"score"`
	ScoreErrorRate    float64 `json:"scoreErrorRate"`
	ScoreP90LatencyMs float64 `json:"scoreP90LatencyMs"`
	ScoreHeadLag      uint64  `json:"scoreHeadLag"`
	ScoreSamples      int     `json:"scoreSamples"`
	OnCanonical       bool    `json:"onCanonicalFork"`
}

type networkHealth struct {
	ID            string `json:"id"`
	UpstreamCount int    `json:"upstreamCount"`
	HealthyCount  int    `json:"healthyCount"`
	FinalizedSlot uint64 `json:"finalizedSlot"`
	CanonicalSlot uint64 `json:"canonicalSlot"`
	CanonicalRoot string `json:"canonicalRoot"`
}

type cacheStats struct {
	Network string `json:"network"`
	Enabled bool   `json:"enabled"`
	Size    int    `json:"size"`
}

type cacheEntryInfo struct {
	Network     string      `json:"network"`
	Key         string      `json:"key"`
	Status      int         `json:"status"`
	Headers     http.Header `json:"headers,omitempty"`
	BodySize    int         `json:"bodySize"`
	CreatedAt   time.Time   `json:"createdAt"`
	ExpiresAt   *time.Time  `json:"expiresAt,omitempty"`
	BodyBase64  string      `json:"bodyBase64,omitempty"`
	ContentType string      `json:"contentType,omitempty"`
}

type cacheListResult struct {
	Total   int              `json:"total"`
	Entries []cacheEntryInfo `json:"entries"`
}

type cacheDeleteResult struct {
	Network string `json:"network"`
	Key     string `json:"key"`
	Deleted bool   `json:"deleted"`
}

type sessionStats struct {
	Network        string         `json:"network"`
	ActiveSessions int            `json:"activeSessions"`
	StickyCounts   map[string]int `json:"stickyCounts,omitempty"`
}

type forkInfo struct {
	Network       string         `json:"network"`
	CanonicalSlot uint64         `json:"canonicalSlot"`
	CanonicalRoot string         `json:"canonicalRoot"`
	MaxSlot       uint64         `json:"maxSlot"`
	Upstreams     []forkUpstream `json:"upstreams"`
}

type forkUpstream struct {
	ID          string `json:"id"`
	HeadSlot    uint64 `json:"headSlot"`
	HeadRoot    string `json:"headRoot"`
	OnCanonical bool   `json:"onCanonicalFork"`
	ForkStatus  string `json:"forkStatus"`
	ClientType  string `json:"clientType"`
}

func (s *StatusAPI) handleHealth(w http.ResponseWriter, r *http.Request) {
	var result []networkHealth
	for id, n := range s.Networks {
		pool := n.Pool()
		all := pool.All()
		healthy := 0
		for _, u := range all {
			if u.IsHealthy() {
				healthy++
			}
		}
		canonSlot, canonRoot := pool.BlockCache().CanonicalHead()
		result = append(result, networkHealth{
			ID:            id,
			UpstreamCount: len(all),
			HealthyCount:  healthy,
			FinalizedSlot: pool.FinalizedSlot(),
			CanonicalSlot: canonSlot,
			CanonicalRoot: canonRoot,
		})
	}
	sortNetworkHealth(result)
	writeJSON(w, result)
}

func (s *StatusAPI) handleUpstreams(w http.ResponseWriter, r *http.Request) {
	var result []upstreamStatus
	for id, n := range s.Networks {
		pool := n.Pool()
		bc := pool.BlockCache()
		for _, u := range pool.All() {
			score := pool.ScoreDetails(u)
			health := "up"
			if !u.IsHealthy() {
				health = "down"
			} else if !u.IsReady() || u.Health() == upstream.HealthDegraded {
				health = "degraded"
			}
			result = append(result, upstreamStatus{
				ID:                u.ID,
				ObfuscatedID:      networkpkg.ObfuscateUpstreamID(u.ID),
				Network:           id,
				URL:               sanitizeUpstreamURL(u.URL),
				LoadBalancing:     score.LoadBalancing,
				Health:            health,
				HeadSlot:          u.HeadSlot(),
				HeadRoot:          u.HeadRoot(),
				SyncDistance:      u.SyncDistance(),
				ClientType:        u.ClientType(),
				ActiveConn:        u.ActiveConns(),
				Priority:          u.Priority,
				Weight:            u.Weight,
				Archive:           u.IsArchive(),
				Score:             score.Score,
				ScoreErrorRate:    score.ErrorRate,
				ScoreP90LatencyMs: float64(score.P90Latency) / float64(time.Millisecond),
				ScoreHeadLag:      score.HeadLag,
				ScoreSamples:      score.Samples,
				OnCanonical:       bc.IsOnCanonicalFork(u.ID),
			})
		}
	}
	sortUpstreamStatuses(result)
	writeJSON(w, result)
}

func (s *StatusAPI) handleCache(w http.ResponseWriter, r *http.Request) {
	var result []cacheStats
	for id, n := range s.Networks {
		c := n.CacheInstance()
		cs := cacheStats{Network: id, Enabled: c != nil}
		if c != nil {
			cs.Size = c.Size()
		}
		result = append(result, cs)
	}
	sortCacheStats(result)
	writeJSON(w, result)
}

func (s *StatusAPI) handleCacheEntries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listCacheEntries(w, r)
	case http.MethodDelete:
		s.deleteCacheEntry(w, r)
	default:
		w.Header().Set("Allow", "GET, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *StatusAPI) listCacheEntries(w http.ResponseWriter, r *http.Request) {
	networkFilter := strings.TrimSpace(r.URL.Query().Get("network"))
	keyFilter := strings.TrimSpace(r.URL.Query().Get("key"))
	includeBody := parseBoolQuery(r.URL.Query().Get("includeBody"))
	// Hard cap to avoid very large responses; client-side search/pagination handles the rest.
	const maxEntries = 2000

	var all []cacheEntryInfo
	for id, n := range s.Networks {
		if networkFilter != "" && id != networkFilter {
			continue
		}
		c := n.CacheInstance()
		if c == nil {
			continue
		}
		for _, entry := range c.Entries(0, includeBody) {
			if keyFilter != "" && entry.Key != keyFilter {
				continue
			}
			item := cacheEntryInfo{
				Network:     id,
				Key:         entry.Key,
				Status:      entry.Status,
				Headers:     debuglog.SanitizeHeaders(entry.Headers),
				BodySize:    entry.BodySize,
				CreatedAt:   entry.Created,
				ContentType: entry.Headers.Get("Content-Type"),
			}
			if !entry.Expires.IsZero() {
				expiresAt := entry.Expires
				item.ExpiresAt = &expiresAt
			}
			if includeBody {
				item.BodyBase64 = base64.StdEncoding.EncodeToString(entry.Body)
			}
			all = append(all, item)
			if len(all) >= maxEntries {
				break
			}
		}
		if len(all) >= maxEntries {
			break
		}
	}
	sortCacheEntries(all)
	if all == nil {
		all = []cacheEntryInfo{}
	}
	writeJSON(w, cacheListResult{Total: len(all), Entries: all})
}

func (s *StatusAPI) deleteCacheEntry(w http.ResponseWriter, r *http.Request) {
	networkID := strings.TrimSpace(r.URL.Query().Get("network"))
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if networkID == "" || key == "" {
		http.Error(w, "network and key are required", http.StatusBadRequest)
		return
	}
	network, ok := s.Networks[networkID]
	if !ok {
		http.Error(w, "network not found", http.StatusNotFound)
		return
	}
	c := network.CacheInstance()
	if c == nil {
		http.Error(w, "cache not enabled", http.StatusNotFound)
		return
	}

	deleted := c.Get(key) != nil
	if deleted {
		c.Delete(key)
	}
	writeJSON(w, cacheDeleteResult{Network: networkID, Key: key, Deleted: deleted})
}

func (s *StatusAPI) handleSessions(w http.ResponseWriter, r *http.Request) {
	var result []sessionStats
	for id, n := range s.Networks {
		sess := n.Sessions()
		ss := sessionStats{Network: id}
		if sess != nil {
			ss.ActiveSessions = sess.ActiveSessions()
			ss.StickyCounts = sess.StickyCounts()
		}
		result = append(result, ss)
	}
	sortSessionStats(result)
	writeJSON(w, result)
}

func (s *StatusAPI) handleForks(w http.ResponseWriter, r *http.Request) {
	var result []forkInfo
	for id, n := range s.Networks {
		pool := n.Pool()
		bc := pool.BlockCache()
		canonSlot, canonRoot := bc.CanonicalHead()

		fi := forkInfo{
			Network:       id,
			CanonicalSlot: canonSlot,
			CanonicalRoot: canonRoot,
			MaxSlot:       bc.MaxSlot(),
		}
		for _, u := range pool.All() {
			fi.Upstreams = append(fi.Upstreams, forkUpstream{
				ID:          u.ID,
				HeadSlot:    u.HeadSlot(),
				HeadRoot:    u.HeadRoot(),
				OnCanonical: bc.IsOnCanonicalFork(u.ID),
				ForkStatus:  bc.ForkStatus(u.ID),
				ClientType:  u.ClientType(),
			})
		}
		sortForkUpstreams(fi.Upstreams)
		result = append(result, fi)
	}
	sortForkInfos(result)
	writeJSON(w, result)
}

func sortNetworkHealth(items []networkHealth) {
	slices.SortFunc(items, func(a, b networkHealth) int { return cmp.Compare(a.ID, b.ID) })
}

func sortUpstreamStatuses(items []upstreamStatus) {
	slices.SortFunc(items, func(a, b upstreamStatus) int {
		return cmp.Or(cmp.Compare(a.Network, b.Network), cmp.Compare(a.Priority, b.Priority), cmp.Compare(a.ID, b.ID))
	})
}

func sortCacheStats(items []cacheStats) {
	slices.SortFunc(items, func(a, b cacheStats) int { return cmp.Compare(a.Network, b.Network) })
}

func sortCacheEntries(items []cacheEntryInfo) {
	slices.SortFunc(items, func(a, b cacheEntryInfo) int {
		return cmp.Or(cmp.Compare(a.Network, b.Network), b.CreatedAt.Compare(a.CreatedAt), cmp.Compare(a.Key, b.Key))
	})
}

func sortSessionStats(items []sessionStats) {
	slices.SortFunc(items, func(a, b sessionStats) int { return cmp.Compare(a.Network, b.Network) })
}

func sortForkInfos(items []forkInfo) {
	slices.SortFunc(items, func(a, b forkInfo) int { return cmp.Compare(a.Network, b.Network) })
}

func sortForkUpstreams(items []forkUpstream) {
	slices.SortFunc(items, func(a, b forkUpstream) int {
		return cmp.Or(cmp.Compare(b.HeadSlot, a.HeadSlot), cmp.Compare(a.ID, b.ID))
	})
}

func parseBoolQuery(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (s *StatusAPI) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML)) //nolint:errcheck
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// sanitizeUpstreamURL strips userinfo, path, query, and fragment from an
// upstream URL so credentials embedded in any of those components do not leak
// to dashboard clients. Providers like QuickNode embed API keys in the path.
func sanitizeUpstreamURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	scheme := u.Scheme
	if scheme == "" {
		scheme = "http"
	}
	return scheme + "://" + u.Host
}

//go:embed dashboard.html
var dashboardHTML string
