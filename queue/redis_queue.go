package queue

import "github.com/redis/go-redis/v9"

const (
	keySeq     = "queue:seq"
	keyZ       = "queue:z"
	keyUserSeq = "queue:userSeq"
)

type RedisQueue struct {
	rdb       *redis.Client
	euqueueFn *redis.Script
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
