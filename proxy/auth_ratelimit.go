package proxy

import (
	"net/http"

	"github.com/mysticryuujin/ebeacon/config"
	"golang.org/x/time/rate"
)

func buildAuthRateLimiters(auth *config.AuthConfig) (keyLimiters, tierLimiters map[string]*rate.Limiter) {
	if auth == nil {
		return nil, nil
	}
	if len(auth.Tiers) > 0 {
		tierLimiters = make(map[string]*rate.Limiter, len(auth.Tiers))
		for _, tier := range auth.Tiers {
			if tier.RateLimiting != nil && tier.RateLimiting.Limit > 0 {
				burst := tier.RateLimiting.Burst
				if burst <= 0 {
					burst = int(tier.RateLimiting.Limit)
				}
				tierLimiters[tier.Name] = rate.NewLimiter(rate.Limit(tier.RateLimiting.Limit), burst)
			}
		}
	}
	if len(auth.Keys) > 0 {
		keyLimiters = make(map[string]*rate.Limiter, len(auth.Keys))
		for _, key := range auth.Keys {
			if key.RateLimiting != nil && key.RateLimiting.Limit > 0 {
				burst := key.RateLimiting.Burst
				if burst <= 0 {
					burst = int(key.RateLimiting.Limit)
				}
				keyLimiters[key.ID] = rate.NewLimiter(rate.Limit(key.RateLimiting.Limit), burst)
			}
		}
	}
	return keyLimiters, tierLimiters
}

func applyAuthRateLimits(w http.ResponseWriter, keyLimiters, tierLimiters map[string]*rate.Limiter, result AuthResult) bool {
	if result.KeyID == "" {
		return true
	}
	if limiter, ok := keyLimiters[result.KeyID]; ok {
		// A key with its own rate limit is exempt from the tier pool — the
		// per-key limit is a full replacement, not an additive check.  This
		// lets operators give specific keys a dedicated quota (higher or lower)
		// without sharing the tier's aggregate bucket.
		if !limiter.Allow() {
			w.Header().Set("X-Ebeacon-Rate-Limited", "per-key")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return false
		}
		return true
	}
	if result.Tier == "" {
		return true
	}
	if limiter, ok := tierLimiters[result.Tier]; ok {
		if !limiter.Allow() {
			w.Header().Set("X-Ebeacon-Rate-Limited", "tier:"+result.Tier)
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return false
		}
	}
	return true
}
