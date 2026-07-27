package upstream

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
)

type syncStatusResponse struct {
	Data struct {
		HeadSlot     string `json:"head_slot"`
		SyncDistance string `json:"sync_distance"`
		IsSyncing    bool   `json:"is_syncing"`
		ElOffline    bool   `json:"el_offline"`
	} `json:"data"`
}

type finalityCheckpointsResponse struct {
	Data struct {
		Finalized struct {
			Epoch string `json:"epoch"`
		} `json:"finalized"`
	} `json:"data"`
}

type nodeVersionResponse struct {
	Data struct {
		Version string `json:"version"`
	} `json:"data"`
}

type beaconHeaderResponse struct {
	Data struct {
		Root   string `json:"root"`
		Header struct {
			Message struct {
				Slot       string `json:"slot"`
				ParentRoot string `json:"parent_root"`
			} `json:"message"`
		} `json:"header"`
	} `json:"data"`
}

var clientTypePatterns = []struct {
	re       *regexp.Regexp
	typeName string
}{
	{regexp.MustCompile(`(?i)^Lighthouse/`), ClientLighthouse},
	{regexp.MustCompile(`(?i)^Prysm/`), ClientPrysm},
	{regexp.MustCompile(`(?i)^teku/`), ClientTeku},
	{regexp.MustCompile(`(?i)^Nimbus/`), ClientNimbus},
	{regexp.MustCompile(`(?i)^Lodestar/`), ClientLodestar},
	{regexp.MustCompile(`(?i)^Grandine/`), ClientGrandine},
	{regexp.MustCompile(`(?i)^Caplin/`), ClientCaplin},
}

// HealthMonitor runs background health checks against all upstreams in a pool.
type HealthMonitor struct {
	upstreams []*Upstream
	pool      *Pool
	cfg       config.HealthConfig
}

func newHealthMonitor(upstreams []*Upstream, pool *Pool, cfg config.HealthConfig) *HealthMonitor {
	return &HealthMonitor{upstreams: upstreams, pool: pool, cfg: cfg}
}

func (h *HealthMonitor) recordProbeSuccess(u *Upstream, started time.Time) {
	u.RecordSuccess(time.Since(started))
	h.pool.RefreshUpstreamScoreMetrics(u)
}

func (h *HealthMonitor) recordProbeError(u *Upstream) {
	u.RecordScoreError()
	h.pool.RefreshUpstreamScoreMetrics(u)
}

func probeRequest(ctx context.Context, u *Upstream, path string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.URL+path, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range u.Headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (h *HealthMonitor) start(ctx context.Context) {
	for _, u := range h.upstreams {
		go h.monitorSync(ctx, u)
		go h.monitorFinality(ctx, u)
		go h.monitorVersion(ctx, u)
		go h.monitorHead(ctx, u)
		go h.monitorNodeHealth(ctx, u)
	}
	go h.cleanupBlockCache(ctx)
}

// monitorSync polls /eth/v1/node/syncing at checkInterval.
func (h *HealthMonitor) monitorSync(ctx context.Context, u *Upstream) {
	h.checkSync(ctx, u)
	ticker := time.NewTicker(h.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.checkSync(ctx, u)
		}
	}
}

// monitorFinality polls /eth/v1/beacon/states/head/finality_checkpoints at finalityInterval.
func (h *HealthMonitor) monitorFinality(ctx context.Context, u *Upstream) {
	h.checkFinality(ctx, u)
	ticker := time.NewTicker(h.cfg.FinalityInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.checkFinality(ctx, u)
		}
	}
}

func (h *HealthMonitor) checkSync(ctx context.Context, u *Upstream) {
	started := time.Now()
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := probeRequest(checkCtx, u, "/eth/v1/node/syncing")
	if err != nil {
		u.setSyncVerdict(HealthDown)
		return
	}

	resp, err := u.Client.Do(req)
	if err != nil {
		slog.Warn("health check failed", "network", u.NetworkID, "upstream", u.ID, "err", err)
		u.setSyncVerdict(HealthDown)
		h.recordProbeError(u)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		slog.Warn("health check bad status", "network", u.NetworkID, "upstream", u.ID, "status", resp.StatusCode)
		u.setSyncVerdict(HealthDown)
		h.recordProbeError(u)
		return
	}

	var s syncStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		slog.Error("health check decode error", "network", u.NetworkID, "upstream", u.ID, "err", err)
		u.setSyncVerdict(HealthDown)
		h.recordProbeError(u)
		return
	}

	headSlot, headErr := strconv.ParseUint(s.Data.HeadSlot, 10, 64)
	syncDistance, distErr := strconv.ParseUint(s.Data.SyncDistance, 10, 64)
	if headErr != nil || distErr != nil {
		slog.Error("health check decode error", "network", u.NetworkID, "upstream", u.ID,
			"head_slot", s.Data.HeadSlot, "sync_distance", s.Data.SyncDistance)
		u.setSyncVerdict(HealthDown)
		h.recordProbeError(u)
		return
	}
	// Nodes can report is_syncing=false while still many slots behind (e.g. they
	// consider themselves synced to a minority fork). Apply our own threshold so
	// we don't route validator traffic to a node that is effectively lagging.
	// el_offline means the beacon node still serves CL data but cannot follow
	// the head reliably — degraded, not down.
	isSyncing := s.Data.IsSyncing || syncDistance > h.cfg.MaxSyncDistance || s.Data.ElOffline

	u.UpdateSyncStatus(isSyncing, headSlot, syncDistance)
	h.recordProbeSuccess(u, started)

	slog.Debug("health check ok",
		"network", u.NetworkID, "upstream", u.ID,
		"head_slot", headSlot, "sync_distance", syncDistance,
		"is_syncing", s.Data.IsSyncing, "el_offline", s.Data.ElOffline)
}

func (h *HealthMonitor) checkFinality(ctx context.Context, u *Upstream) {
	if !u.IsHealthy() {
		return
	}
	started := time.Now()

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := probeRequest(checkCtx, u, "/eth/v1/beacon/states/head/finality_checkpoints")
	if err != nil {
		return
	}

	resp, err := u.Client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close() //nolint:errcheck
		}
		h.recordProbeError(u)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	var fc finalityCheckpointsResponse
	if err := json.NewDecoder(resp.Body).Decode(&fc); err != nil {
		h.recordProbeError(u)
		return
	}

	epoch, err := strconv.ParseUint(fc.Data.Finalized.Epoch, 10, 64)
	if err != nil {
		h.recordProbeError(u)
		return
	}

	u.UpdateFinalizedEpoch(epoch)
	h.pool.UpdateFinalizedEpoch(epoch)
	h.recordProbeSuccess(u, started)

	slog.Debug("finality updated",
		"network", u.NetworkID, "upstream", u.ID, "finalized_epoch", epoch)
}

func (h *HealthMonitor) monitorVersion(ctx context.Context, u *Upstream) {
	h.checkVersion(ctx, u)
	// Client type (Lighthouse, Teku, etc.) never changes at runtime; 5-minute
	// polling is just a safety net in case a node is replaced in-place.
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.checkVersion(ctx, u)
		}
	}
}

func (h *HealthMonitor) checkVersion(ctx context.Context, u *Upstream) {
	started := time.Now()
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := probeRequest(checkCtx, u, "/eth/v1/node/version")
	if err != nil {
		return
	}

	resp, err := u.Client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close() //nolint:errcheck
		}
		h.recordProbeError(u)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	var v nodeVersionResponse
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		h.recordProbeError(u)
		return
	}

	for _, p := range clientTypePatterns {
		if p.re.MatchString(v.Data.Version) {
			u.SetClientType(p.typeName)
			h.recordProbeSuccess(u, started)
			return
		}
	}
	u.SetClientType(ClientUnknown)
	h.recordProbeSuccess(u, started)
}

// monitorHead polls /eth/v1/beacon/headers/head at checkInterval to track
// each upstream's view of the chain tip. We poll rather than subscribe to SSE
// here because SSE streams are managed per-client in sse.go; the health monitor
// needs its own independent view to detect fork divergence across upstreams.
func (h *HealthMonitor) monitorHead(ctx context.Context, u *Upstream) {
	h.checkHead(ctx, u)
	ticker := time.NewTicker(h.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.checkHead(ctx, u)
		}
	}
}

func (h *HealthMonitor) checkHead(ctx context.Context, u *Upstream) {
	if !u.IsHealthy() {
		return
	}
	started := time.Now()

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := probeRequest(checkCtx, u, "/eth/v1/beacon/headers/head")
	if err != nil {
		return
	}

	resp, err := u.Client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close() //nolint:errcheck
		}
		h.recordProbeError(u)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	var bh beaconHeaderResponse
	if err := json.NewDecoder(resp.Body).Decode(&bh); err != nil {
		h.recordProbeError(u)
		return
	}

	slot, err := strconv.ParseUint(bh.Data.Header.Message.Slot, 10, 64)
	if err != nil {
		h.recordProbeError(u)
		return
	}

	u.UpdateHeadBlock(slot, bh.Data.Root, bh.Data.Header.Message.ParentRoot)
	h.pool.blockCache.AddBlock(u.ID, slot, bh.Data.Root, bh.Data.Header.Message.ParentRoot)
	h.pool.SyncCanonicalHead()
	h.recordProbeSuccess(u, started)
	h.pool.RefreshScoreMetrics()

	slog.Debug("head block updated",
		"network", u.NetworkID, "upstream", u.ID, "slot", slot, "root", bh.Data.Root)
}

func (h *HealthMonitor) monitorNodeHealth(ctx context.Context, u *Upstream) {
	h.checkNodeHealth(ctx, u)
	ticker := time.NewTicker(h.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.checkNodeHealth(ctx, u)
		}
	}
}

// checkNodeHealth polls /eth/v1/node/health. The Beacon API spec defines only
// 200 (ready), 206 (syncing), and 503 (not ready) as health verdicts, so only
// 503 (or a transport failure) latches the upstream down. Any other status
// (404/429/etc.) means the endpoint isn't answering the health question — a
// path-restricted gateway or a provider rate-limiting /health — so we clear
// the latch and defer to the sync poller rather than excluding a node that
// serves every other Beacon path. A 200 is not applied here either;
// monitorSync is authoritative for the up/degraded distinction.
func (h *HealthMonitor) checkNodeHealth(ctx context.Context, u *Upstream) {
	started := time.Now()
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := probeRequest(checkCtx, u, "/eth/v1/node/health")
	if err != nil {
		return
	}

	resp, err := u.Client.Do(req)
	if err != nil {
		slog.Warn("node health check failed", "network", u.NetworkID, "upstream", u.ID, "err", err)
		u.setNodeVerdict(HealthDown)
		h.recordProbeError(u)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	switch resp.StatusCode {
	case http.StatusOK: // 200: ready — defer to monitorSync for exact status
		u.setNodeVerdict(HealthUp)
		h.recordProbeSuccess(u, started)
	case http.StatusPartialContent: // 206: syncing
		u.setNodeVerdict(HealthDegraded)
		h.recordProbeSuccess(u, started)
	case http.StatusServiceUnavailable: // 503: not ready
		slog.Warn("node health check unhealthy", "network", u.NetworkID, "upstream", u.ID, "status", resp.StatusCode)
		u.setNodeVerdict(HealthDown)
		h.recordProbeError(u)
	default: // endpoint not answering the health question — defer to sync poller
		slog.Debug("node health check returned unexpected status, deferring to sync poll",
			"network", u.NetworkID, "upstream", u.ID, "status", resp.StatusCode)
		u.setNodeVerdict(HealthUp)
	}
}

func (h *HealthMonitor) cleanupBlockCache(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.pool.blockCache.Cleanup()
		}
	}
}
