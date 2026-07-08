package state

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/redis/go-redis/v9"
)

const (
	headChannel        = "ebeacon:head"
	finalizedKeyPrefix = "ebeacon:finalized_epoch:"
)

func finalizedKeyFor(network string) string {
	return finalizedKeyPrefix + network
}

// finalizedEpochScript sets the key only when the new epoch is higher, so a
// lagging instance cannot regress the shared value.
var finalizedEpochScript = redis.NewScript(`
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
if tonumber(ARGV[1]) > cur then redis.call('SET', KEYS[1], ARGV[1]) return 1 end
return 0`)

// RedisState uses Redis pub/sub for cross-instance coordination.
type RedisState struct {
	client      *redis.Client
	finalizedMu sync.Mutex
	finalized   map[string]uint64
	headCh      chan HeadUpdate
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewRedisState connects to Redis and starts subscribing.
func NewRedisState(cfg *config.RedisStateConfig) (*RedisState, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
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

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	rs := &RedisState{
		client:    client,
		finalized: make(map[string]uint64),
		headCh:    make(chan HeadUpdate, 64),
		cancel:    cancel,
	}

	rs.wg.Add(1)
	go rs.subscribe(ctx)

	return rs, nil
}

func (rs *RedisState) subscribe(ctx context.Context) {
	defer rs.wg.Done()
	defer close(rs.headCh)

	pubsub := rs.client.Subscribe(ctx, headChannel)
	defer func() {
		if err := pubsub.Close(); err != nil {
			slog.Warn("redis pubsub close error", "err", err)
		}
	}()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var update HeadUpdate
			if err := json.Unmarshal([]byte(msg.Payload), &update); err != nil {
				continue
			}
			if update.Network == "" || update.Root == "" || update.Slot == 0 {
				continue
			}
			select {
			case rs.headCh <- update:
			default:
			}
		}
	}
}

func (rs *RedisState) PublishHead(network string, slot uint64, root string) {
	payload, err := json.Marshal(HeadUpdate{Network: network, Slot: slot, Root: root})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rs.client.Publish(ctx, headChannel, payload).Err(); err != nil {
		slog.Warn("redis publish head failed", "err", err)
	}
}

func (rs *RedisState) SubscribeHead() <-chan HeadUpdate {
	return rs.headCh
}

func (rs *RedisState) PublishFinalized(network string, epoch uint64) {
	rs.finalizedMu.Lock()
	if epoch <= rs.finalized[network] {
		rs.finalizedMu.Unlock()
		return
	}
	rs.finalized[network] = epoch
	rs.finalizedMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := finalizedEpochScript.Run(ctx, rs.client, []string{finalizedKeyFor(network)}, epoch).Err(); err != nil {
		slog.Warn("redis set finalized epoch failed", "network", network, "err", err)
	}
}

func (rs *RedisState) GetFinalized(network string) uint64 {
	var remote uint64
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if val, err := rs.client.Get(ctx, finalizedKeyFor(network)).Result(); err == nil {
		if epoch, err := strconv.ParseUint(val, 10, 64); err == nil {
			remote = epoch
		}
	}

	rs.finalizedMu.Lock()
	defer rs.finalizedMu.Unlock()
	if remote > rs.finalized[network] {
		rs.finalized[network] = remote
	}
	return rs.finalized[network]
}

func (rs *RedisState) Close() error {
	rs.cancel()
	rs.wg.Wait()
	return rs.client.Close()
}
