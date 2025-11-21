package queue

import (
	"context"
	"fmt"

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

func toInt64(v interface{}) (int64, error) {
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
}
