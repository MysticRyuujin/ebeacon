package network

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ebeacon/ebeacon/upstream"
)

const headCacheWarmTimeout = 5 * time.Second

type parsedCacheKey struct {
	method       string
	pathQuery    string
	path         string
	scope        string
	acceptBinary bool
}

func parseCacheKey(key string) (parsedCacheKey, bool) {
	var out parsedCacheKey

	idx := strings.IndexByte(key, ':')
	if idx < 0 || idx == len(key)-1 {
		return out, false
	}
	key = key[idx+1:]

	idx = strings.IndexByte(key, ':')
	if idx < 0 || idx == len(key)-1 {
		return out, false
	}
	out.method = key[:idx]
	rest := key[idx+1:]

	end := len(rest)
	for _, sep := range []string{":accept=", ":upstream="} {
		if j := strings.Index(rest, sep); j >= 0 && j < end {
			end = j
		}
	}
	out.pathQuery = rest[:end]
	if out.pathQuery == "" {
		return out, false
	}

	suffix := rest[end:]
	out.acceptBinary = strings.Contains(suffix, ":accept=binary")
	if j := strings.Index(suffix, ":upstream="); j >= 0 {
		out.scope = suffix[j+len(":upstream="):]
	}

	out.path = out.pathQuery
	if j := strings.IndexByte(out.path, '?'); j >= 0 {
		out.path = out.path[:j]
	}

	return out, true
}

func (n *Network) warmHeadCache(ctx context.Context, source *upstream.Upstream, keys []string) {
	if n == nil || n.cache == nil || len(keys) == 0 {
		return
	}

	warmCtx, cancel := context.WithTimeout(ctx, headCacheWarmTimeout)
	defer cancel()

	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		if err := n.populateCacheKey(warmCtx, key, source); err != nil && warmCtx.Err() == nil {
			slog.Debug("head watcher: pre-warm failed",
				"network", n.id,
				"key", key,
				"err", err)
		}
	}
}

func (n *Network) populateCacheKey(ctx context.Context, key string, source *upstream.Upstream) error {
	parsed, ok := parseCacheKey(key)
	if !ok || parsed.method != http.MethodGet || !pathHasNamedSlotID(parsed.path) {
		return nil
	}

	policy := n.cache.Policy(parsed.method, parsed.path)
	if policy == nil {
		return nil
	}

	reqURL, err := url.Parse(parsed.pathQuery)
	if err != nil {
		return fmt.Errorf("parse cache key URL: %w", err)
	}

	req := &http.Request{
		Method: parsed.method,
		URL:    reqURL,
		Header: make(http.Header),
	}
	if parsed.acceptBinary {
		req.Header.Set("Accept", "application/octet-stream")
	}

	preferID := ""
	required := requiredUpstreamSelector{}
	if parsed.scope != "" {
		required = requiredSelectorFromValue(parsed.scope)
	} else if source != nil {
		preferID = source.ID
	}

	resp, _, err := n.executeWithFailsafe(
		ctx,
		req,
		nil,
		preferID,
		required,
		n.effectiveFailsafe(parsed.method, parsed.path),
		normalizeAPIPath(parsed.path),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil
	}

	body, err := readAndFinalizeResponseBody(resp)
	if err != nil {
		return fmt.Errorf("read pre-warm response body: %w", err)
	}

	ttl := n.effectiveCacheTTL(policy.TTL(), parsed.path)
	n.cache.Set(buildCacheKey(n.id, req, parsed.scope), resp.StatusCode, resp.Header, body, ttl)
	return nil
}
