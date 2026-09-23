package cache

import (
	"time"
)

// Store is the backend storage interface for the cache.
type Store interface {
	Get(key string) (*Entry, bool)
	Set(key string, entry *Entry, ttl time.Duration)
	Delete(key string)
	// PromoteIf sets TTL to forever for live, expiring entries whose key
	// satisfies fn and returns how many it promoted.
	PromoteIf(fn func(key string) bool) int
	Entries(limit int, includeBody bool) []*Entry
	Keys() []string
	Len() int
}
