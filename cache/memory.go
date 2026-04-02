package cache

import (
	"container/list"
	"sync"
	"time"
)

// MemoryStore is an in-memory LRU cache with TTL expiration.
type MemoryStore struct {
	mu      sync.Mutex
	items   map[string]*list.Element
	lru     *list.List
	maxSize int
}

// NewMemoryStore creates a MemoryStore with the given max entry count.
func NewMemoryStore(maxSize int) *MemoryStore {
	if maxSize <= 0 {
		maxSize = 2048
	}
	return &MemoryStore{
		items:   make(map[string]*list.Element, maxSize),
		lru:     list.New(),
		maxSize: maxSize,
	}
}

func (m *MemoryStore) Get(key string) (*Entry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	el, ok := m.items[key]
	if !ok {
		return nil, false
	}

	e := el.Value.(*Entry)
	if !e.expires.IsZero() && time.Now().After(e.expires) {
		m.lru.Remove(el)
		delete(m.items, key)
		return nil, false
	}

	m.lru.MoveToFront(el)
	return e, true
}

func (m *MemoryStore) Set(key string, entry *Entry, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if el, ok := m.items[key]; ok {
		m.lru.MoveToFront(el)
		el.Value = entry
		return
	}

	for m.lru.Len() >= m.maxSize {
		tail := m.lru.Back()
		if tail == nil {
			break
		}
		old := tail.Value.(*Entry)
		m.lru.Remove(tail)
		delete(m.items, old.key)
	}

	el := m.lru.PushFront(entry)
	m.items[key] = el
}

func (m *MemoryStore) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.items[key]; ok {
		m.lru.Remove(el)
		delete(m.items, key)
	}
}

func (m *MemoryStore) Promote(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.items[key]; ok {
		el.Value.(*Entry).expires = time.Time{}
	}
}

func (m *MemoryStore) Entries(limit int, includeBody bool) []*Entry {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	entries := make([]*Entry, 0, m.lru.Len())
	for el := m.lru.Front(); el != nil; {
		next := el.Next()
		entry := el.Value.(*Entry)
		if !entry.expires.IsZero() && now.After(entry.expires) {
			m.lru.Remove(el)
			delete(m.items, entry.key)
			el = next
			continue
		}
		entries = append(entries, cloneEntry(entry, includeBody))
		if limit > 0 && len(entries) >= limit {
			break
		}
		el = next
	}
	return entries
}

func (m *MemoryStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lru.Len()
}
