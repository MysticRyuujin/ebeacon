package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/redis/go-redis/v9"
)

// redisEntry is a serializable form of Entry for Redis storage.
type redisEntry struct {
	Key     string
	Status  int
	Headers map[string][]string
	Body    []byte
	Created time.Time
}

// RedisStore is a Redis-backed cache store.
type RedisStore struct {
	client    *redis.Client
	keyPrefix string
}

// NewRedisStore creates a RedisStore from the given config.
func NewRedisStore(cfg *config.RedisCacheConfig) (*RedisStore, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, err
	}
	if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	if cfg.DB != 0 {
		opts.DB = cfg.DB
	}
	if cfg.MaxRetries > 0 {
		opts.MaxRetries = cfg.MaxRetries
	}
	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &RedisStore{client: client, keyPrefix: cfg.KeyPrefix}, nil
}

func (r *RedisStore) prefixed(key string) string {
	return r.keyPrefix + key
}

func (r *RedisStore) Get(key string) (*Entry, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	data, err := r.client.Get(ctx, r.prefixed(key)).Bytes()
	if err != nil {
		return nil, false
	}

	e, err := decodeRedisPayload(data)
	if err != nil {
		return nil, false
	}
	return e, true
}

func (r *RedisStore) Set(key string, entry *Entry, ttl time.Duration) {
	re := redisEntry{
		Key:     entry.key,
		Status:  entry.status,
		Headers: map[string][]string(entry.headers),
		Body:    entry.body,
		Created: entry.created,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&re); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if ttl <= 0 {
		ttl = 24 * time.Hour * 365
	}
	if err := r.client.Set(ctx, r.prefixed(key), buf.Bytes(), ttl).Err(); err != nil {
		slog.Warn("redis cache set failed", "key", key, "err", err)
	}
}

func (r *RedisStore) Delete(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.client.Del(ctx, r.prefixed(key)).Err(); err != nil {
		slog.Warn("redis cache delete failed", "key", key, "err", err)
	}
}

func (r *RedisStore) Promote(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.client.Persist(ctx, r.prefixed(key)).Err(); err != nil {
		slog.Warn("redis cache promote failed", "key", key, "err", err)
	}
}

func (r *RedisStore) Entries(limit int, includeBody bool) []*Entry {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pattern := r.prefixed("*")
	var cursor uint64
	entries := make([]*Entry, 0)
	for {
		keys, next, err := r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			break
		}
		for _, key := range keys {
			data, err := r.client.Get(ctx, key).Bytes()
			if err != nil {
				continue
			}
			entry, err := decodeRedisPayload(data)
			if err != nil {
				continue
			}
			ttl, err := r.client.TTL(ctx, key).Result()
			if err == nil {
				switch {
				case ttl > 0:
					entry.expires = time.Now().Add(ttl)
				case ttl == -1:
					entry.expires = time.Time{}
				case ttl == -2:
					continue
				}
			}
			entries = append(entries, entry)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].created.Equal(entries[j].created) {
			return entries[i].key < entries[j].key
		}
		return entries[i].created.After(entries[j].created)
	})
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries
}

func (r *RedisStore) Len() int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var count int
	var cursor uint64
	pattern := r.prefixed("*")
	for {
		keys, next, err := r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return 0
		}
		count += len(keys)
		if next == 0 {
			break
		}
		cursor = next
	}
	return count
}

func decodeRedisPayload(data []byte) (*Entry, error) {
	var re redisEntry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&re); err != nil {
		return nil, err
	}
	return &Entry{
		key:     re.Key,
		status:  re.Status,
		headers: http.Header(re.Headers),
		body:    re.Body,
		created: re.Created,
	}, nil
}
