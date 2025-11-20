package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var enterScript = redis.NewScript(`
local userKey = KEYS[1]
local nextTicketKey = KEYS[2]

local t = redis.call("GET", userKey)
if t then
	return tonumber(t)
end

local newTicket = redis.call("INCR", nextTicketKey)
redis.call("SET", userKey, newTicket)
return newTicket
`)

type QueueService struct {
	rdb             *redis.Client
	eventID         string
	admitRatePerSec int64 // 초당 몇명까지 입장시킬지 (ETA 계산)
}

func NewQueueService(rdb *redis.Client, eventID string, admitRatePerSec int64) *QueueService {
	return &QueueService{
		rdb:             rdb,
		eventID:         eventID,
		admitRatePerSec: admitRatePerSec,
	}
}

func (q *QueueService) keyNextTicket() string {
	return "event:" + q.eventID + ":next_ticket"
}

func (q *QueueService) keyNowServing() string {
	return "event:" + q.eventID + ":now_serving"
}

func (q *QueueService) keyUser(userID string) string {
	return "event:" + q.eventID + ":user:" + userID
}

type QueueStatus struct {
	Ticket     int64 `json:"ticket"`
	Position   int64 `json:"position"`
	ETASeconds int64 `json:"eta_seconds"`
	NowServing int64 `json:"now_serving"`
	NextTicket int64 `json:"next_ticket"`
	AdmitRate  int64 `json:"admit_rate_per_sec"`
}

func (q *QueueService) Enter(userID string) (*QueueStatus, error) {
	userKey := q.keyUser(userID)
	nextTicketKey := q.keyNextTicket()

	res, err := enterScript.Run(ctx, q.rdb, []string{userKey, nextTicketKey}).Result()

	if err != nil {
		return nil, err
	}

	ticket := res.(int64)

	nowServing, err := q.rdb.Get(ctx, q.keyNowServing()).Int64()

	if err != nil {
		return nil, err
	}

	nextTicket, err := q.rdb.Get(ctx, nextTicketKey).Int64()

	// go 에서는 key 가 없을때 redis.Nil 을 리턴
	if err == redis.Nil {
		nextTicket = ticket
	} else if err != nil {
		return nil, err
	}

	position := ticket - nowServing

	if position < 0 {
		position = 0
	}

	var eta int64
	if q.admitRatePerSec > 0 {
		eta = position / q.admitRatePerSec
	}

	return &QueueStatus{
		Ticket:     ticket,
		Position:   position,
		ETASeconds: eta,
		NowServing: nowServing,
		NextTicket: nextTicket,
		AdmitRate:  q.admitRatePerSec,
	}, nil
}

func (q *QueueService) Status(userID string) (*QueueStatus, error) {
	userKey := q.keyUser(userID)

	ticket, err := q.rdb.Get(ctx, userKey).Int64()

	if err == redis.Nil {
		// 대기열에 없는 상태임
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	nowServing, err := q.rdb.Get(ctx, q.keyNowServing()).Int64()

	if err != nil {
		return nil, err
	}

	nextTicket, err := q.rdb.Get(ctx, q.keyNextTicket()).Int64()

	if err == redis.Nil {
		nextTicket = ticket
	} else if err != nil {
		return nil, err
	}

	position := ticket - nowServing

	if position < 0 {
		position = 0
	}

	var eta int64
	if q.admitRatePerSec > 0 {
		eta = position / q.admitRatePerSec
	}

	return &QueueStatus{
		Ticket:     ticket,
		Position:   position,
		ETASeconds: eta,
		NowServing: nowServing,
		NextTicket: nextTicket,
		AdmitRate:  q.admitRatePerSec,
	}, nil
}

func (q *QueueService) Admin(count int64) error {
	if count <= 0 {
		return nil
	}

	return q.rdb.IncrBy(ctx, q.keyNowServing(), count).Err()
}
