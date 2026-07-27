package cache

import (
	"time"
)

// Store is the backend storage interface for the cache.
type Store interface {
	Get(key string) (*Entry, bool)
	Set(key string, entry *Entry, ttl time.Duration)
	Delete(key string)
	Promote(key string) // set TTL to forever
	Entries(limit int, includeBody bool) []*Entry
	Keys() []string
	Len() int
}
