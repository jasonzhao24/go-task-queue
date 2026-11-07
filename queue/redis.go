package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type TaskState int

const (
	Available TaskState = iota
	Leased
)

type Task struct {
	ID         string
	Payload    []byte
	State      TaskState
	WorkerID   string
	VisibleAt  time.Time
	RetryCount int
	CreatedAt  int64
}

type RedisQueue struct {
	Client     *redis.Client // Capitalized
	QueueKey   string        // Capitalized
	PayloadKey string        // Capitalized
}

func NewRedisQueue(ctx context.Context, addr string) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		PoolSize: 1000,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &RedisQueue{
		Client:     client,
		QueueKey:   "tasks:queue",
		PayloadKey: "tasks:payloads",
	}, nil
}

func (r *RedisQueue) Enqueue(ctx context.Context, payload []byte) (*Task, error) {
	taskID := fmt.Sprintf("%d", time.Now().UnixNano())

	pipe := r.Client.Pipeline()
	pipe.HSet(ctx, r.PayloadKey, taskID, payload)
	pipe.ZAdd(ctx, r.QueueKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: taskID})
	_, err := pipe.Exec(ctx)

	return &Task{ID: taskID, Payload: payload}, err
}

var dequeueScript = redis.NewScript(`
local queueKey = KEYS[1]
local currentTime = tonumber(ARGV[1])
local timeout = tonumber(ARGV[2])

local tasks = redis.call('ZRANGEBYSCORE', queueKey, '-inf', currentTime, 'LIMIT', 0, 1)
if #tasks > 0 then
	local taskID = tasks[1]
	redis.call('ZADD', queueKey, timeout, taskID)
	return taskID
end
return nil
`)

func (r *RedisQueue) Dequeue(ctx context.Context, workerID string) (*Task, error) {
	now := time.Now().UnixMilli()
	timeout := now + 2000 // 2-second visibility timeout

	taskID, err := dequeueScript.Run(ctx, r.Client, []string{r.QueueKey}, now, timeout).Result()
	if err == redis.Nil || taskID == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	idStr := taskID.(string)
	payload, err := r.Client.HGet(ctx, r.PayloadKey, idStr).Bytes()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	return &Task{ID: idStr, Payload: payload, WorkerID: workerID}, nil
}

func (r *RedisQueue) Acknowledge(ctx context.Context, taskID string) error {
	pipe := r.Client.Pipeline()
	zrem := pipe.ZRem(ctx, r.QueueKey, taskID)
	pipe.HDel(ctx, r.PayloadKey, taskID)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	if zrem.Val() == 0 {
		return redis.Nil
	}
	return nil
}
