package state

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/redis/go-redis/v9"
)

const (
	headChannel  = "ebeacon:head"
	finalizedKey = "ebeacon:finalized_epoch"
)

// RedisState uses Redis pub/sub for cross-instance coordination.
type RedisState struct {
	client         *redis.Client
	finalizedEpoch atomic.Uint64
	headCh         chan HeadUpdate
	cancel         context.CancelFunc
	wg             sync.WaitGroup
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
		client: client,
		headCh: make(chan HeadUpdate, 64),
		cancel: cancel,
	}

	// Load initial finalized epoch
	val, err := client.Get(ctx, finalizedKey).Result()
	if err == nil {
		if epoch, err := strconv.ParseUint(val, 10, 64); err == nil {
			rs.finalizedEpoch.Store(epoch)
		}
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

func (rs *RedisState) PublishFinalized(epoch uint64) {
	for {
		old := rs.finalizedEpoch.Load()
		if epoch <= old {
			return
		}
		if rs.finalizedEpoch.CompareAndSwap(old, epoch) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := rs.client.Set(ctx, finalizedKey, strconv.FormatUint(epoch, 10), 0).Err(); err != nil {
				slog.Warn("redis set finalized epoch failed", "err", err)
			}
			cancel()
			return
		}
	}
}

func (rs *RedisState) GetFinalized() uint64 {
	return rs.finalizedEpoch.Load()
}

func (rs *RedisState) Close() error {
	rs.cancel()
	rs.wg.Wait()
	return rs.client.Close()
}
