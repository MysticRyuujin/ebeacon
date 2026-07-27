package network

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mysticryuujin/ebeacon/upstream"
	"golang.org/x/time/rate"
)

// Session tracks per-client IP state: rate limiting and upstream stickiness.
type Session struct {
	mu       sync.Mutex
	ip       string
	limiter  *rate.Limiter // nil = no rate limiting
	stickyID string
	lastSeen time.Time
}

func (s *Session) Allow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSeen = time.Now()
	if s.limiter == nil {
		return true
	}
	return s.limiter.Allow()
}

func (s *Session) StickyUpstream() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stickyID
}

func (s *Session) SetSticky(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stickyID = id
}

// SessionManager manages per-IP sessions with optional rate limiting and stickiness.
type SessionManager struct {
	mu      sync.RWMutex
	byIP    map[string]*Session
	lim     rate.Limit
	burst   int
	timeout time.Duration
}

func newSessionManager(ratePerSec float64, burst int, sessionTimeout time.Duration) *SessionManager {
	lim := rate.Inf
	if ratePerSec > 0 {
		lim = rate.Limit(ratePerSec)
	}
	if burst == 0 {
		burst = 1
	}
	sm := &SessionManager{
		byIP:    make(map[string]*Session),
		lim:     lim,
		burst:   burst,
		timeout: sessionTimeout,
	}
	return sm
}

func (sm *SessionManager) Get(r *http.Request) *Session {
	ip := ClientIP(r)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	s, ok := sm.byIP[ip]
	if !ok {
		var lim *rate.Limiter
		if sm.lim != rate.Inf {
			lim = rate.NewLimiter(sm.lim, sm.burst)
		}
		s = &Session{ip: ip, limiter: lim, lastSeen: time.Now()}
		sm.byIP[ip] = s
		return s
	}
	s.mu.Lock()
	s.lastSeen = time.Now()
	s.mu.Unlock()
	return s
}

// StartCleanup launches a background goroutine that evicts idle sessions.
// It stops when ctx is cancelled.
func (sm *SessionManager) StartCleanup(ctx context.Context) {
	go func() {
		interval := sm.timeout / 2
		if interval < time.Minute {
			interval = time.Minute
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cutoff := time.Now().Add(-sm.timeout)
				sm.mu.Lock()
				for ip, s := range sm.byIP {
					s.mu.Lock()
					last := s.lastSeen
					s.mu.Unlock()
					if last.Before(cutoff) {
						delete(sm.byIP, ip)
					}
				}
				sm.mu.Unlock()
			}
		}
	}()
}

// StartRebalancer launches a background goroutine that periodically redistributes
// sticky sessions from overloaded to underloaded upstreams.
func (sm *SessionManager) StartRebalancer(ctx context.Context, interval time.Duration, pool *upstream.Pool, threshold float64, maxSweep int) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sm.rebalance(pool, threshold, maxSweep)
			}
		}
	}()
}

func (sm *SessionManager) rebalance(pool *upstream.Pool, threshold float64, maxSweep int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Count sessions per sticky upstream
	counts := make(map[string]int)
	for _, s := range sm.byIP {
		s.mu.Lock()
		id := s.stickyID
		s.mu.Unlock()
		if id != "" {
			counts[id]++
		}
	}

	if len(counts) == 0 {
		return
	}

	total := 0
	for _, c := range counts {
		total += c
	}
	mean := float64(total) / float64(len(counts))
	maxLoad := mean * threshold

	// Find underloaded upstreams
	allUps := pool.All()
	var underloaded []string
	for _, u := range allUps {
		if u.IsReady() && float64(counts[u.ID]) < mean {
			underloaded = append(underloaded, u.ID)
		}
	}
	if len(underloaded) == 0 {
		return
	}

	moved := 0
	uidx := 0
	for _, s := range sm.byIP {
		if moved >= maxSweep {
			break
		}
		s.mu.Lock()
		if s.stickyID != "" && float64(counts[s.stickyID]) > maxLoad {
			oldID := s.stickyID
			newID := underloaded[uidx%len(underloaded)]
			s.stickyID = newID
			counts[oldID]--
			counts[newID]++
			uidx++
			moved++
		}
		s.mu.Unlock()
	}

	if moved > 0 {
		slog.Debug("session rebalance", "moved", moved)
	}
}

// ActiveSessions returns the number of active sessions.
func (sm *SessionManager) ActiveSessions() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.byIP)
}

// StickyCounts returns a map of upstream ID -> sticky session count.
func (sm *SessionManager) StickyCounts() map[string]int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	counts := make(map[string]int)
	for _, s := range sm.byIP {
		s.mu.Lock()
		if s.stickyID != "" {
			counts[s.stickyID]++
		}
		s.mu.Unlock()
	}
	return counts
}

// trustedProxies holds the CIDR set whose X-Forwarded-For / X-Real-IP headers
// are honored. nil = legacy behavior: trust forwarding headers from any peer.
var trustedProxies atomic.Pointer[[]netip.Prefix]

// SetTrustedProxies configures which peers' forwarding headers ClientIP
// honors. Entries may be CIDRs or bare IPs. Empty resets to the legacy
// trust-everyone behavior; forwarding headers are then spoofable, so
// deployments exposed directly to clients should set server.trustedProxies.
func SetTrustedProxies(cidrs []string) error {
	if len(cidrs) == 0 {
		trustedProxies.Store(nil)
		return nil
	}
	prefixes := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if p, err := netip.ParsePrefix(c); err == nil {
			prefixes = append(prefixes, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(c)
		if err != nil {
			return fmt.Errorf("invalid trusted proxy %q (want CIDR or IP)", c)
		}
		prefixes = append(prefixes, netip.PrefixFrom(a, a.BitLen()))
	}
	trustedProxies.Store(&prefixes)
	return nil
}

func isTrustedProxyAddr(prefixes []netip.Prefix, ip string) bool {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	a = a.Unmap()
	for _, p := range prefixes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

func remoteAddrHost(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// ClientIP extracts the originating IP from the request. When trusted proxies
// are configured, forwarding headers are honored only from trusted peers, and
// the X-Forwarded-For chain is walked right-to-left past trusted hops so a
// client cannot spoof its own address into per-IP rate limiting or sessions.
func ClientIP(r *http.Request) string {
	prefixes := trustedProxies.Load()
	if prefixes == nil {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if i := strings.IndexByte(xff, ','); i >= 0 {
				return strings.TrimSpace(xff[:i])
			}
			return strings.TrimSpace(xff)
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return strings.TrimSpace(xri)
		}
		return remoteAddrHost(r)
	}

	peer := remoteAddrHost(r)
	if !isTrustedProxyAddr(*prefixes, peer) {
		return peer
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		for i := len(parts) - 1; i >= 0; i-- {
			entry := strings.TrimSpace(parts[i])
			if entry == "" {
				continue
			}
			if !isTrustedProxyAddr(*prefixes, entry) {
				return entry
			}
		}
		if first := strings.TrimSpace(parts[0]); first != "" {
			return first
		}
	}
	if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" {
		return xri
	}
	return peer
}
