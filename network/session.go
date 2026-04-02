package network

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ebeacon/ebeacon/upstream"
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
	}
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

// ClientIP extracts the originating IP from the request.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}
