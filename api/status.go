package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ebeacon/ebeacon/config"
	networkpkg "github.com/ebeacon/ebeacon/network"
	"github.com/ebeacon/ebeacon/proxy"
	"github.com/ebeacon/ebeacon/upstream"
)

// StatusAPI serves JSON status endpoints for the web UI.
type StatusAPI struct {
	Networks map[string]*networkpkg.Network
	Auth     *config.AuthConfig // optional: protect dashboard with auth
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
				URL:               u.URL,
				LoadBalancing:     score.LoadBalancing,
				Health:            health,
				HeadSlot:          u.HeadSlot(),
				HeadRoot:          u.HeadRoot(),
				SyncDistance:      u.SyncDistance(),
				ClientType:        u.ClientType(),
				ActiveConn:        u.ActiveConns(),
				Priority:          u.Priority,
				Weight:            u.Weight,
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
				Headers:     entry.Headers,
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
	sort.Slice(items, func(i, j int) bool {
		return items[i].ID < items[j].ID
	})
}

func sortUpstreamStatuses(items []upstreamStatus) {
	sort.Slice(items, func(i, j int) bool {
		left := items[i]
		right := items[j]
		if left.Network != right.Network {
			return left.Network < right.Network
		}
		if left.Priority != right.Priority {
			return left.Priority < right.Priority
		}
		return left.ID < right.ID
	})
}

func sortCacheStats(items []cacheStats) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].Network < items[j].Network
	})
}

func sortCacheEntries(items []cacheEntryInfo) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Network != items[j].Network {
			return items[i].Network < items[j].Network
		}
		if !items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].CreatedAt.After(items[j].CreatedAt)
		}
		return items[i].Key < items[j].Key
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

func sortSessionStats(items []sessionStats) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].Network < items[j].Network
	})
}

func sortForkInfos(items []forkInfo) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].Network < items[j].Network
	})
}

func sortForkUpstreams(items []forkUpstream) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].HeadSlot != items[j].HeadSlot {
			return items[i].HeadSlot > items[j].HeadSlot
		}
		return items[i].ID < items[j].ID
	})
}

func (s *StatusAPI) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML)) //nolint:errcheck
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

var dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>eBeacon Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f1117;color:#e1e4e8;padding:20px}
h1{font-size:1.5rem;margin-bottom:20px;color:#58a6ff}
h2{font-size:1.1rem;margin:20px 0 10px;color:#8b949e}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;margin-bottom:24px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}
.card h3{font-size:0.9rem;color:#8b949e;margin-bottom:8px}
.table-stack{display:grid;gap:16px;margin-top:8px}
.table-card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}
.table-header{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:8px}
.table-meta{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 10px}
.table-count{color:#8b949e;font-size:0.8rem}
.table-scroll{overflow-x:auto}
.controls{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px}
.controls input,.controls select,.controls button{background:#0d1117;border:1px solid #30363d;color:#e1e4e8;border-radius:6px;padding:8px 10px;font-size:0.85rem}
.controls input,.controls select{min-height:36px}
.controls button{cursor:pointer}
.controls button:hover{border-color:#58a6ff}
.stack{display:grid;gap:12px}
.stat{font-size:1.8rem;font-weight:600;color:#58a6ff}
table{width:100%;border-collapse:collapse;margin-top:8px}
th,td{text-align:left;padding:8px 12px;border-bottom:1px solid #21262d;font-size:0.85rem}
th{color:#8b949e;font-weight:500}
.sort-button{display:inline-flex;align-items:center;gap:6px;background:none;border:none;color:inherit;font:inherit;cursor:pointer;padding:0}
.sort-button.active{color:#e1e4e8}
.sort-indicator{font-size:0.7rem;color:#58a6ff;min-width:10px}
.empty-state{color:#8b949e}
.mono{font-family:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace;word-break:break-all}
.action-button{background:#1f6feb;border:1px solid #1f6feb;color:#fff;border-radius:6px;padding:6px 10px;font-size:0.8rem;cursor:pointer}
.action-button:hover{background:#388bfd;border-color:#388bfd}
.action-button.danger{background:#da3633;border-color:#da3633}
.action-button.danger:hover{background:#f85149;border-color:#f85149}
.preview-panel{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:12px}
.preview-meta{display:grid;gap:4px;color:#8b949e;font-size:0.8rem;margin-bottom:8px}
.preview-body{background:#010409;border:1px solid #21262d;border-radius:6px;padding:12px;max-height:360px;overflow:auto;white-space:pre-wrap;word-break:break-word}
.up{color:#3fb950}.down{color:#f85149}.degraded{color:#d29922}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:0.75rem;font-weight:500}
.badge.up{background:#0d3117}.badge.down{background:#3d1114}.badge.degraded{background:#3d2e00}
.canonical{color:#3fb950}.non-canonical{color:#f85149}.lagging{color:#d29922}
.summary-chip{display:inline-flex;align-items:center;gap:6px;background:#0d1117;border:1px solid #21262d;border-radius:999px;color:#8b949e;font-size:0.75rem;padding:4px 10px}
.cell-meta{display:block;color:#8b949e;font-size:0.75rem;margin-top:2px;white-space:nowrap}
#refresh{position:fixed;top:16px;right:16px;background:#238636;color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;font-size:0.85rem}
#refresh:hover{background:#2ea043}
@media (max-width: 720px){body{padding:16px}th,td{padding:8px}}
</style>
</head>
<body>
<h1>eBeacon Dashboard</h1>
<button id="refresh" onclick="loadAll()">Refresh</button>
<div id="health-section"><h2>Network Health</h2><div class="grid" id="health"></div></div>
<h2>Upstreams</h2><div class="table-stack" id="upstreams-by-network"></div>
<h2>Cache</h2><div class="grid" id="cache-stats"></div>
<h2>Cache Entries</h2><div class="table-stack" id="cache-entries"></div>
<h2>Sessions</h2><div class="grid" id="sessions"></div>
<script>
const base = window.location.pathname.replace(/\/$/,'');
const upstreamColumns = [
	{key:'id',label:'ID',type:'string'},
	{key:'obfuscatedId',label:'Token',type:'string'},
	{key:'clientType',label:'Client',type:'string'},
	{key:'health',label:'Health',type:'health'},
	{key:'score',label:'Score',type:'number'},
	{key:'headSlot',label:'Head Slot',type:'number'},
	{key:'activeConnections',label:'Active',type:'number'},
	{key:'priority',label:'Priority',type:'number'},
	{key:'fork',label:'Fork',type:'fork'}
];
const defaultUpstreamSort = {key:'priority',direction:'asc'};
const upstreamSortState = {};
const cacheEntriesState = {network:'',search:'',page:0,pageSize:25};
let latestUpstreams = [];
let latestForks = [];
let latestCacheStats = [];
let latestCacheEntries = [];
let activeCachePreview = null;

function escapeHTML(value){
	return String(value == null ? '' : value)
		.replace(/&/g,'&amp;')
		.replace(/</g,'&lt;')
		.replace(/>/g,'&gt;')
		.replace(/"/g,'&quot;')
		.replace(/'/g,'&#39;');
}

function formatFixed(value,digits){
	const number = Number(value);
	if(!Number.isFinite(number)){
		return (0).toFixed(digits);
	}
	return number.toFixed(digits);
}

function formatPercent(value){
	return formatFixed((Number(value) || 0) * 100, 1)+'%';
}

function formatLatencyMs(value){
	const number = Number(value) || 0;
	if(number >= 1000){
		return formatFixed(number / 1000, 2)+'s';
	}
	if(number >= 100){
		return formatFixed(number, 0)+'ms';
	}
	return formatFixed(number, 1)+'ms';
}

function getDefaultUpstreamSort(upstreams){
	const usesScore = (upstreams || []).some(function(upstream){
		return upstream.loadBalancing === 'score';
	});
	if(usesScore){
		return {key:'score',direction:'desc'};
	}
	return defaultUpstreamSort;
}

function getUpstreamSortValue(upstream,key){
	if(key==='health'){
		const rank={up:0,degraded:1,down:2};
		return rank[upstream.health] ?? 99;
	}
	if(key==='fork'){
		return upstream.onCanonicalFork ? 0 : 1;
	}
	return upstream[key];
}

function sortIndicator(network,key){
	const state = upstreamSortState[network];
	if(!state || state.key !== key){
		return '';
	}
	return state.direction === 'asc' ? '&uarr;' : '&darr;';
}

function sortUpstreams(network,upstreams){
	const state = upstreamSortState[network] || getDefaultUpstreamSort(upstreams);
	const column = upstreamColumns.find(function(entry){ return entry.key === state.key; }) || upstreamColumns[0];
	const direction = state.direction === 'desc' ? -1 : 1;
	return upstreams.slice().sort(function(left,right){
		const leftValue = getUpstreamSortValue(left,state.key);
		const rightValue = getUpstreamSortValue(right,state.key);
		if(column.type === 'number' || column.type === 'health' || column.type === 'fork'){
			const leftNumber = Number(leftValue || 0);
			const rightNumber = Number(rightValue || 0);
			if(leftNumber !== rightNumber){
				return (leftNumber - rightNumber) * direction;
			}
		} else {
			const leftText = String(leftValue || '').toLowerCase();
			const rightText = String(rightValue || '').toLowerCase();
			if(leftText < rightText){
				return -1 * direction;
			}
			if(leftText > rightText){
				return 1 * direction;
			}
		}
		return String(left.id || '').localeCompare(String(right.id || ''));
	});
}

function renderUpstreamRow(upstream){
	const scoreSummary = 'err '+formatPercent(upstream.scoreErrorRate)+' | p90 '+formatLatencyMs(upstream.scoreP90LatencyMs)+' | lag '+escapeHTML(upstream.scoreHeadLag)+' | sync '+escapeHTML(upstream.syncDistance)+' | samples '+escapeHTML(upstream.scoreSamples);
	return '<tr>'+
		'<td>'+escapeHTML(upstream.id)+'</td>'+
		'<td class="mono" title="X-Ebeacon-Upstream token">'+escapeHTML(upstream.obfuscatedId)+'</td>'+
		'<td>'+escapeHTML(upstream.clientType)+'</td>'+
		'<td><span class="badge '+escapeHTML(upstream.health)+'">'+escapeHTML(upstream.health)+'</span></td>'+
		'<td title="'+escapeHTML(scoreSummary)+'">'+escapeHTML(formatFixed(upstream.score,3))+'<span class="cell-meta">'+escapeHTML('p90 '+formatLatencyMs(upstream.scoreP90LatencyMs)+' | err '+formatPercent(upstream.scoreErrorRate))+'</span></td>'+
		'<td>'+escapeHTML(upstream.headSlot)+'</td>'+
		'<td>'+escapeHTML(upstream.activeConnections)+'</td>'+
		'<td>'+escapeHTML(upstream.priority)+'</td>'+
		'<td class="'+(upstream.forkStatus === 'canonical' ? 'canonical' : upstream.forkStatus === 'lagging' ? 'lagging' : 'non-canonical')+'">'+(upstream.forkStatus || (upstream.onCanonicalFork ? 'canonical' : 'forked'))+'</td>'+
	'</tr>';
}

function getForkInfoByNetwork(forks){
	const lookup = {};
	(forks || []).forEach(function(fork){
		lookup[fork.network] = fork;
	});
	return lookup;
}

function renderForkSummary(forkInfo, upstreamCount){
	if(!forkInfo){
		return '';
	}
	const canonicalCount = (forkInfo.upstreams || []).filter(function(upstream){
		return upstream.onCanonicalFork;
	}).length;
	const laggingCount = (forkInfo.upstreams || []).filter(function(u){ return u.forkStatus === 'lagging'; }).length;
	const forkedCount = Math.max((upstreamCount || 0) - canonicalCount - laggingCount, 0);
	return '<div class="table-meta">'+
		'<span class="summary-chip">Canonical slot '+escapeHTML(forkInfo.canonicalSlot)+'</span>'+
		'<span class="summary-chip">Max slot '+escapeHTML(forkInfo.maxSlot)+'</span>'+
		'<span class="summary-chip"><span class="canonical">'+canonicalCount+' canonical</span></span>'+
		(laggingCount ? '<span class="summary-chip"><span class="lagging">'+laggingCount+' lagging</span></span>' : '')+
		'<span class="summary-chip"><span class="'+(forkedCount ? 'non-canonical' : 'canonical')+'">'+forkedCount+' forked</span></span>'+
	'</div>';
}

function renderUpstreams(upstreams){
	const grouped = {};
	const forksByNetwork = getForkInfoByNetwork(latestForks);
	(upstreams || []).forEach(function(upstream){
		const network = upstream.network || 'unknown';
		if(!grouped[network]){
			grouped[network] = [];
		}
		var fi = forksByNetwork[network];
		if(fi && fi.upstreams){
			var fu = fi.upstreams.find(function(u){ return u.id === upstream.id; });
			if(fu){ upstream.forkStatus = fu.forkStatus; }
		}
		grouped[network].push(upstream);
		if(!upstreamSortState[network]){
			upstreamSortState[network] = getDefaultUpstreamSort(grouped[network]);
		}
	});
	const networks = Object.keys(grouped).sort(function(left,right){ return left.localeCompare(right); });
	const container = document.getElementById('upstreams-by-network');
	if(!networks.length){
		container.innerHTML = '<div class="table-card empty-state">No upstreams configured.</div>';
		return;
	}
	container.innerHTML = networks.map(function(network){
		const state = upstreamSortState[network] || getDefaultUpstreamSort(grouped[network]);
		const sorted = sortUpstreams(network,grouped[network]);
		const forkSummary = renderForkSummary(forksByNetwork[network], sorted.length);
		const routingMode = (grouped[network][0] && grouped[network][0].loadBalancing) ? grouped[network][0].loadBalancing : 'round-robin';
		const routingSummary = '<div class="table-meta"><span class="summary-chip">Routing '+escapeHTML(routingMode)+'</span></div>';
		const headers = upstreamColumns.map(function(column){
			const active = state.key === column.key;
			const sortValue = active ? (state.direction === 'asc' ? 'ascending' : 'descending') : 'none';
			return '<th aria-sort="'+sortValue+'"><button type="button" class="sort-button'+(active ? ' active' : '')+'" data-network="'+escapeHTML(network)+'" data-key="'+column.key+'">'+column.label+'<span class="sort-indicator">'+sortIndicator(network,column.key)+'</span></button></th>';
		}).join('');
		return '<section class="table-card">'+
			'<div class="table-header"><h3>'+escapeHTML(network)+'</h3><span class="table-count">'+sorted.length+' upstream'+(sorted.length === 1 ? '' : 's')+'</span></div>'+
			forkSummary+
			routingSummary+
			'<div class="table-scroll"><table><thead><tr>'+headers+'</tr></thead><tbody>'+sorted.map(renderUpstreamRow).join('')+'</tbody></table></div>'+
		'</section>';
	}).join('');
	container.querySelectorAll('.sort-button').forEach(function(button){
		button.addEventListener('click',function(event){
			const target = event.currentTarget;
			const network = target.getAttribute('data-network');
			const key = target.getAttribute('data-key');
			const state = upstreamSortState[network] || defaultUpstreamSort;
			if(state.key === key){
				upstreamSortState[network] = {key:key,direction:state.direction === 'asc' ? 'desc' : 'asc'};
			} else {
				upstreamSortState[network] = {key:key,direction:'asc'};
			}
			renderUpstreams(latestUpstreams);
		});
	});
}

function formatTimestamp(value){
	if(!value){
		return 'never';
	}
	const date = new Date(value);
	if(Number.isNaN(date.getTime())){
		return '';
	}
	return date.toLocaleString();
}

function syncCacheEntryFiltersFromDOM(){
	const network = document.getElementById('cache-network-filter');
	const search = document.getElementById('cache-search-filter');
	if(network){
		cacheEntriesState.network = network.value || '';
	}
	if(search){
		cacheEntriesState.search = search.value.trim();
	}
}

function buildCacheEntriesURL(){
	return base+'/api/cache/entries';
}

function buildSingleCacheEntryURL(network,key){
	const params = new URLSearchParams({network:network,key:key,includeBody:'true'});
	return base+'/api/cache/entries?'+params.toString();
}

function filterCacheEntries(entries){
	const network = cacheEntriesState.network;
	const search = cacheEntriesState.search.toLowerCase();
	return (entries || []).filter(function(entry){
		if(network && entry.network !== network){
			return false;
		}
		if(search && entry.key.toLowerCase().indexOf(search) === -1){
			return false;
		}
		return true;
	});
}

function decodeBase64(value){
	try{
		return atob(value);
	}catch(_error){
		return '';
	}
}

function formatPreviewBody(entry){
	if(!entry || !entry.bodyBase64){
		return '';
	}
	const decoded = decodeBase64(entry.bodyBase64);
	if(decoded === ''){
		return '(unable to decode body)';
	}
	try{
		return JSON.stringify(JSON.parse(decoded), null, 2);
	}catch(_error){
		return decoded;
	}
}

function updateCacheNetworkOptions(caches){
	const select = document.getElementById('cache-network-filter');
	const search = document.getElementById('cache-search-filter');
	if(!select){
		return;
	}
	const selected = cacheEntriesState.network;
	const options = ['<option value="">All networks</option>'];
	(caches || []).forEach(function(cache){
		options.push('<option value="'+escapeHTML(cache.network)+'">'+escapeHTML(cache.network)+'</option>');
	});
	select.innerHTML = options.join('');
	select.value = selected;
	if(select.value !== selected){
		cacheEntriesState.network = select.value || '';
	}
	if(search && search.value !== cacheEntriesState.search){
		search.value = cacheEntriesState.search;
	}
	if(!select.dataset.bound){
		select.addEventListener('change',function(){
			cacheEntriesState.page = 0;
			syncCacheEntryFiltersFromDOM();
			renderCacheEntries(latestCacheEntries);
		});
		select.dataset.bound = 'true';
	}
	if(search && !search.dataset.bound){
		search.addEventListener('keydown',function(event){
			if(event.key === 'Enter'){
				event.preventDefault();
				cacheEntriesState.page = 0;
				syncCacheEntryFiltersFromDOM();
				renderCacheEntries(latestCacheEntries);
			}
		});
		search.dataset.bound = 'true';
	}
}

function renderCacheEntries(entries){
	const container = document.getElementById('cache-entries');
	if(!container){
		return;
	}
	const filtered = filterCacheEntries(entries);
	const total = filtered.length;
	const pageSize = cacheEntriesState.pageSize;
	const page = cacheEntriesState.page;
	const totalPages = Math.max(1, Math.ceil(total / pageSize));
	const clampedPage = Math.min(page, totalPages - 1);
	if(clampedPage !== page){
		cacheEntriesState.page = clampedPage;
	}
	const startEntry = total === 0 ? 0 : clampedPage * pageSize + 1;
	const endEntry = Math.min((clampedPage + 1) * pageSize, total);
	const pageEntries = filtered.slice(clampedPage * pageSize, (clampedPage + 1) * pageSize);
	const rows = pageEntries.length ? pageEntries.map(function(entry){
		return '<tr>'+
			'<td>'+escapeHTML(entry.network)+'</td>'+
			'<td class="mono">'+escapeHTML(entry.key)+'</td>'+
			'<td>'+escapeHTML(entry.status)+'</td>'+
			'<td>'+escapeHTML(entry.contentType || '')+'</td>'+
			'<td>'+escapeHTML(entry.bodySize)+'</td>'+
			'<td>'+escapeHTML(formatTimestamp(entry.createdAt))+'</td>'+
			'<td>'+escapeHTML(formatTimestamp(entry.expiresAt))+'</td>'+
			'<td><div class="controls"><button type="button" class="action-button preview-cache-entry" data-network="'+escapeHTML(entry.network)+'" data-key="'+escapeHTML(entry.key)+'">View</button><button type="button" class="action-button danger delete-cache-entry" data-network="'+escapeHTML(entry.network)+'" data-key="'+escapeHTML(entry.key)+'">Delete</button></div></td>'+
		'</tr>';
	}).join('') : '<tr><td colspan="8" class="empty-state">No cache entries match the current filter.</td></tr>';
	const preview = activeCachePreview ? '<div class="preview-panel">'+
		'<div class="table-header"><h3>Selected Body</h3><button type="button" class="action-button" id="clear-cache-preview">Clear</button></div>'+
		'<div class="preview-meta">'+
			'<div><strong>Network:</strong> '+escapeHTML(activeCachePreview.network)+'</div>'+
			'<div><strong>Key:</strong> <span class="mono">'+escapeHTML(activeCachePreview.key)+'</span></div>'+
			'<div><strong>Content-Type:</strong> '+escapeHTML(activeCachePreview.contentType || '')+'</div>'+
		'</div>'+
		'<pre class="preview-body mono">'+escapeHTML(formatPreviewBody(activeCachePreview))+'</pre>'+
	'</div>' : '';
	const totalAll = (entries || []).length;
	const paginationInfo = total === 0 ? '0 entries' : startEntry+'\u2013'+endEntry+' of '+total+' entr'+(total === 1 ? 'y' : 'ies');
	const pagination = '<div class="controls cache-pagination">'+
		'<button type="button" class="action-button" id="cache-page-prev" '+(clampedPage <= 0 ? 'disabled' : '')+'>&#8592; Prev</button>'+
		'<span class="table-count">'+escapeHTML(paginationInfo)+'</span>'+
		'<button type="button" class="action-button" id="cache-page-next" '+(clampedPage >= totalPages - 1 ? 'disabled' : '')+'>Next &#8594;</button>'+
	'</div>';
	const totalLabel = total === totalAll ? total+' entr'+(total === 1 ? 'y' : 'ies') : total+' match'+(total === 1 ? '' : 'es')+' of '+totalAll;
	container.innerHTML = '<section class="table-card stack">'+
		'<div class="table-header"><h3>Entries</h3><span class="table-count">'+escapeHTML(totalLabel)+'</span></div>'+
		'<div class="controls">'+
			'<select id="cache-network-filter"></select>'+
			'<input id="cache-search-filter" type="text" placeholder="Search key\u2026" value="'+escapeHTML(cacheEntriesState.search)+'">'+
			'<button type="button" onclick="applyCacheEntryFilters()">Apply</button>'+
		'</div>'+
		pagination+
		'<div class="table-scroll"><table><thead><tr><th>Network</th><th>Key</th><th>Status</th><th>Content-Type</th><th>Bytes</th><th>Created</th><th>Expires</th><th>Action</th></tr></thead><tbody>'+rows+'</tbody></table></div>'+
		preview+
	'</section>';
	updateCacheNetworkOptions(latestCacheStats);
	const prevBtn = document.getElementById('cache-page-prev');
	const nextBtn = document.getElementById('cache-page-next');
	if(prevBtn){
		prevBtn.addEventListener('click',function(){
			if(cacheEntriesState.page > 0){
				cacheEntriesState.page--;
				renderCacheEntries(latestCacheEntries);
			}
		});
	}
	if(nextBtn){
		nextBtn.addEventListener('click',function(){
			if(cacheEntriesState.page < totalPages - 1){
				cacheEntriesState.page++;
				renderCacheEntries(latestCacheEntries);
			}
		});
	}
	const clearPreview = document.getElementById('clear-cache-preview');
	if(clearPreview){
		clearPreview.addEventListener('click',function(){
			activeCachePreview = null;
			renderCacheEntries(latestCacheEntries);
		});
	}
	container.querySelectorAll('.preview-cache-entry').forEach(function(button){
		button.addEventListener('click',async function(event){
			const target = event.currentTarget;
			const network = target.getAttribute('data-network');
			const key = target.getAttribute('data-key');
			try{
				target.disabled = true;
				const response = await fetch(buildSingleCacheEntryURL(network,key));
				if(!response.ok){
					throw new Error(await response.text());
				}
				const result = await response.json();
				const items = result && result.entries ? result.entries : (Array.isArray(result) ? result : []);
				activeCachePreview = items.length ? items[0] : null;
				renderCacheEntries(latestCacheEntries);
			}catch(error){
				console.error(error);
				window.alert('Failed to load cache entry body.');
				target.disabled = false;
			}
		});
	});
	container.querySelectorAll('.delete-cache-entry').forEach(function(button){
		button.addEventListener('click',async function(event){
			const target = event.currentTarget;
			const network = target.getAttribute('data-network');
			const key = target.getAttribute('data-key');
			if(!window.confirm('Delete cache entry '+key+' from '+network+'?')){
				return;
			}
			try{
				target.disabled = true;
				const params = new URLSearchParams({network:network,key:key});
				const response = await fetch(base+'/api/cache/entries?'+params.toString(), {method:'DELETE'});
				if(!response.ok){
					throw new Error(await response.text());
				}
				if(activeCachePreview && activeCachePreview.network === network && activeCachePreview.key === key){
					activeCachePreview = null;
				}
				await loadAll();
			}catch(error){
				console.error(error);
				window.alert('Failed to delete cache entry.');
				target.disabled = false;
			}
		});
	});
}

function applyCacheEntryFilters(){
	cacheEntriesState.page = 0;
	syncCacheEntryFiltersFromDOM();
	renderCacheEntries(latestCacheEntries);
}

async function loadAll(){
	try{
		syncCacheEntryFiltersFromDOM();
		const[health,ups,caches,entriesResult,sessions,forks]=await Promise.all([
			fetch(base+'/api/health').then(r=>r.json()),
			fetch(base+'/api/upstreams').then(r=>r.json()),
			fetch(base+'/api/cache').then(r=>r.json()),
			fetch(buildCacheEntriesURL()).then(r=>r.json()),
			fetch(base+'/api/sessions').then(r=>r.json()),
			fetch(base+'/api/forks').then(r=>r.json()),
		]);
		document.getElementById('health').innerHTML=(health||[]).map(function(h){ return '<div class="card"><h3>'+h.id+'</h3><div class="stat">'+h.healthyCount+'/'+h.upstreamCount+'</div><p>Finalized: '+h.finalizedSlot+'</p><p>Canonical: '+h.canonicalSlot+'</p></div>'; }).join('');
		latestUpstreams = ups || [];
		latestForks = forks || [];
		renderUpstreams(latestUpstreams);
		latestCacheStats = caches || [];
		latestCacheEntries = (entriesResult && entriesResult.entries) ? entriesResult.entries : [];
		document.getElementById('cache-stats').innerHTML=(latestCacheStats||[]).map(function(c){ return '<div class="card"><h3>'+c.network+'</h3><div class="stat">'+(c.enabled?c.size+' entries':'disabled')+'</div></div>'; }).join('');
		renderCacheEntries(latestCacheEntries);
		document.getElementById('sessions').innerHTML=(sessions||[]).map(function(s){ return '<div class="card"><h3>'+s.network+'</h3><div class="stat">'+s.activeSessions+' sessions</div></div>'; }).join('');
	}catch(e){
		console.error(e);
	}
}
loadAll();
setInterval(loadAll,5000);
</script>
</body>
</html>`
