package upstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/ebeacon/ebeacon/config"
)

type syncStatusResponse struct {
	Data struct {
		HeadSlot     string `json:"head_slot"`
		SyncDistance string `json:"sync_distance"`
		IsSyncing    bool   `json:"is_syncing"`
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

	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet,
		fmt.Sprintf("%s/eth/v1/node/syncing", u.URL), nil)
	if err != nil {
		u.SetHealth(HealthDown)
		return
	}

	resp, err := u.Client.Do(req)
	if err != nil {
		slog.Warn("health check failed", "network", u.NetworkID, "upstream", u.ID, "err", err)
		u.SetHealth(HealthDown)
		h.recordProbeError(u)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		slog.Warn("health check bad status", "network", u.NetworkID, "upstream", u.ID, "status", resp.StatusCode)
		u.SetHealth(HealthDown)
		h.recordProbeError(u)
		return
	}

	var s syncStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		slog.Error("health check decode error", "network", u.NetworkID, "upstream", u.ID, "err", err)
		u.SetHealth(HealthDown)
		h.recordProbeError(u)
		return
	}

	headSlot, _ := strconv.ParseUint(s.Data.HeadSlot, 10, 64)
	syncDistance, _ := strconv.ParseUint(s.Data.SyncDistance, 10, 64)
	// Nodes can report is_syncing=false while still many slots behind (e.g. they
	// consider themselves synced to a minority fork). Apply our own threshold so
	// we don't route validator traffic to a node that is effectively lagging.
	isSyncing := s.Data.IsSyncing || syncDistance > h.cfg.MaxSyncDistance

	u.UpdateSyncStatus(isSyncing, headSlot, syncDistance)
	h.recordProbeSuccess(u, started)

	slog.Debug("health check ok",
		"network", u.NetworkID, "upstream", u.ID,
		"head_slot", headSlot, "sync_distance", syncDistance, "is_syncing", s.Data.IsSyncing)
}

func (h *HealthMonitor) checkFinality(ctx context.Context, u *Upstream) {
	if !u.IsHealthy() {
		return
	}
	started := time.Now()

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet,
		fmt.Sprintf("%s/eth/v1/beacon/states/head/finality_checkpoints", u.URL), nil)
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

	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet,
		fmt.Sprintf("%s/eth/v1/node/version", u.URL), nil)
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

	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet,
		fmt.Sprintf("%s/eth/v1/beacon/headers/head", u.URL), nil)
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

// checkNodeHealth polls /eth/v1/node/health. A 503 response (or connection
// failure) marks the upstream down; 206 marks it degraded. A 200 is not
// applied here — monitorSync is authoritative for the up/degraded distinction
// so that sync distance thresholds are respected.
func (h *HealthMonitor) checkNodeHealth(ctx context.Context, u *Upstream) {
	started := time.Now()
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet,
		fmt.Sprintf("%s/eth/v1/node/health", u.URL), nil)
	if err != nil {
		return
	}

	resp, err := u.Client.Do(req)
	if err != nil {
		slog.Warn("node health check failed", "network", u.NetworkID, "upstream", u.ID, "err", err)
		u.SetHealth(HealthDown)
		h.recordProbeError(u)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	switch resp.StatusCode {
	case http.StatusOK: // 200: ready — defer to monitorSync for exact status
		h.recordProbeSuccess(u, started)
	case http.StatusPartialContent: // 206: syncing
		u.SetHealth(HealthDegraded)
		h.recordProbeSuccess(u, started)
	default: // 503 or unexpected: not ready
		slog.Warn("node health check unhealthy", "network", u.NetworkID, "upstream", u.ID, "status", resp.StatusCode)
		u.SetHealth(HealthDown)
		h.recordProbeError(u)
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
