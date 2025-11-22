package queue

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	keySeq     = "queue:seq"
	keyZ       = "queue:z"
	keyUserSeq = "queue:userSeq"
)

type RedisQueue struct {
	rdb       *redis.Client
	euqueueFn *redis.Script
}

type JoinResult struct {
	Seq        int64
	Position   int64
	ETASeconds int64
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case string:
		var n int64
		_, err := fmt.Sscan(x, &n)
		return n, err
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

func NewRedisQueue(rdb *redis.Client) *RedisQueue {
	scripts := redis.NewScript(`
		-- KEYS[1] = queue:seq
		-- KEYS[2] = queue:z
		-- KEYS[3] = queue:userSeq
		-- ARGV[1] = userKey

		local existing = redis.call('HGET', KEYS[3], ARGV[1])
		if existing then
			return existing
		end

		local seq = redis.call("INCR", KEYS[1])
		redis.call('ZADD', KEYS[2], seq, ARGV[1])
		redis.call('HSET', KEYS[3], ARGV[1], seq)
		return seq
	`)

	return &RedisQueue{
		rdb:       rdb,
		euqueueFn: scripts,
	}
}

func (q *RedisQueue) getThroughputPerSecond(ctx context.Context) (int64, error) {
	val, err := q.rdb.HGet(ctx, "queue:config", "throughput_per_sec").Result()
	if err != nil {
		return 0, err
	}

	return toInt64(val)
}

func (q *RedisQueue) Join(ctx context.Context, userKey string) (*JoinResult, error) {
	rawSeq, err := q.euqueueFn.Run(ctx, q.rdb, []string{
		keySeq, keyZ, keyUserSeq,
	}, userKey).Result()

	if err != nil {
		return nil, err
	}

	seq, err := toInt64(rawSeq)

	if err != nil {
		return nil, err
	}

	// 포지션을 계산하는 함수
	pos, err := q.rdb.ZCount(ctx, keyZ, "-inf", fmt.Sprintf("%d", seq)).Result()

	if err != nil {
		return nil, err
	}

	throughput, err := q.getThroughputPerSecond(ctx)
	if err != nil || throughput <= 0 {
		throughput = 50
	}
	etaSec := int64(pos) / throughput

	return &JoinResult{
		Seq:        seq,
		Position:   pos,
		ETASeconds: etaSec,
	}, nil
}

func newToken() string {
	return uuid.New().String()
}

func (q *RedisQueue) adminOnce(ctx context.Context) {
	throughput, err := q.getThroughputPerSecond(ctx)
	if err != nil || throughput <= 0 {
		throughput = 50
	}

	users, err := q.rdb.ZRange(ctx, keyZ, 0, throughput-1).Result()
	if err != nil || len(users) == 0 {
		return
	}

	pipe := q.rdb.Pipeline()
	for _, user := range users {
		token := newToken()
		pipe.Set(ctx, "queue:admin:token:"+token, user, 10*time.Minute)
		pipe.ZRem(ctx, keyZ, user)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("failed to execute pipeline: %v", err)
	}
}

func (q *RedisQueue) RunAdmissionWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.adminOnce(ctx)
		}
	}
}
