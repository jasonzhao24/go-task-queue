# Go Distributed Task Queue

A concurrent task queue written in Go and backed by Redis. Workers lease tasks through an atomic Lua script, and tasks held by workers that fail are redelivered automatically when their lease expires, giving at-least-once delivery without a separate reaper process.

In a chaos test with **3M tasks**, **256 concurrent workers**, and a **16.6% per-attempt failure probability**, the queue lost **zero tasks** and sustained **11.3K tasks/sec** at **31 ms p99** dequeue-to-ack latency. See [Chaos Test Results](#chaos-test-results) for exactly what was measured and what was not.

## Architecture

Two Redis keys hold all state:

| Key | Type | Contents |
|---|---|---|
| `tasks:queue` | Sorted set | Member = task ID, score = the time (Unix ms) at which the task becomes visible |
| `tasks:payloads` | Hash | Task ID to payload bytes |

**Enqueue.** The payload is written to the hash and the task ID is added to the sorted set with a score of "now", in one pipelined round trip.

**Dequeue (lease).** A single Lua script runs atomically inside Redis: it finds one task whose score is at or before the current time (`ZRANGEBYSCORE ... LIMIT 0 1`) and immediately rewrites its score to `now + 2000 ms` (`ZADD`). Because Redis executes the script without interleaving other commands, two workers can never lease the same task at the same moment. The payload is then fetched from the hash.

**Acknowledge.** The task is removed from the sorted set and the hash (`ZREM` + `HDEL`). The call reports an error if the task was already gone, so duplicate acks are never counted as completions.

**Failure recovery.** There is no background reaper. A leased task simply has a score in the future. If the worker never acknowledges it, the score passes and the task becomes visible to the next dequeue, which is the visibility-timeout pattern. The lease is 2 seconds.

**Delivery guarantee: at-least-once.** A task is redelivered if a worker fails or is slower than the lease, so consumers should be idempotent.

## Project Layout

```
cmd/api/      HTTP server (enqueue / dequeue / acknowledge)
cmd/chaos/    chaos test and latency profiler
queue/        Redis queue implementation (package queue)
```

## Tech Stack

- Go 1.24, `net/http`
- Redis 7 via [go-redis v9](https://github.com/redis/go-redis), with a Lua script for the atomic lease
- Docker and Docker Compose

## Quick Start

```bash
docker compose up --build
```

The API is available at `http://localhost:8080`, backed by Redis on port 6379.

## API Reference

### Enqueue a task

`POST /enqueue`

```bash
curl -X POST http://localhost:8080/enqueue \
  -H "Content-Type: application/json" \
  -d '{"payload": "encode_video_720p"}'
```

Response:

```json
{"task_id": "1793997012493456300"}
```

### Dequeue (lease) a task

`GET /dequeue?worker_id={worker_id}`

Leases the next visible task for 2 seconds. Returns `204 No Content` when no task is visible.

```bash
curl "http://localhost:8080/dequeue?worker_id=worker-A"
```

Response (the payload is base64-encoded because it is a Go `[]byte`):

```json
{
  "ID": "1793997012493456300",
  "Payload": "ZW5jb2RlX3ZpZGVvXzcyMHA=",
  "WorkerID": "worker-A"
}
```

The response also contains zero-valued `State`, `VisibleAt`, `RetryCount`, and `CreatedAt` fields, which are reserved for upcoming retry support and are not populated yet.

### Acknowledge a task

`POST /acknowledge`

Marks the task complete and deletes it permanently.

```bash
curl -X POST http://localhost:8080/acknowledge \
  -H "Content-Type: application/json" \
  -d '{"task_id": "1793997012493456300"}'
```

Returns `200 OK` on success.

## Chaos Test Results

Run on a single Redis 7 container, with the test binary on the same Docker network.
<!-- TODO: add machine specs (CPU, RAM, OS) here -->

| Metric | Result |
|---|---|
| Tasks enqueued | 3,000,000 |
| Concurrent workers | 256 |
| Failure injection | 16.6% per dequeue attempt |
| Simulated worker failures | 597,233 |
| Unique tasks acknowledged | 3,000,000 |
| Tasks left in the sorted set (`ZCARD`) | 0 |
| Payloads left in the hash (`HLEN`) | 0 |
| Throughput | 11,335 tasks/sec (about 265 s to drain) |
| p99 dequeue-to-ack latency | 31 ms |

### How to read these numbers

- **Zero lost tasks.** After the run, both the sorted set and the payload hash are empty, and the number of unique acknowledgments equals the number enqueued. Only acks where `ZREM` actually removed the task are counted.
- **Throughput** is tasks enqueued divided by the time to drain the queue, measured after the pre-load phase. The run performed 3,597,233 dequeues in total: 3,000,000 successful tasks plus 597,233 failed attempts that had to be redelivered.
- **Latency** is measured from the start of a dequeue to the completion of the acknowledge, for the final successful attempt of each task. It excludes failed attempts and the time a task spends waiting for its lease to expire, so it is not end-to-end task latency.
- **Failures are simulated.** Each worker goroutine skips the acknowledge with probability 0.166 per attempt, which leaves the task to expire and be redelivered. This models a worker that stops mid-task, but no worker processes are actually killed.

### Reproduce

```bash
docker compose run --rm go-api go run ./cmd/chaos
```

The chaos test runs `FLUSHDB` on its Redis instance so that counts are exact. Never point it at a Redis instance you care about.

## Known Limitations and Roadmap

- **Acknowledge is by task ID only.** A worker whose lease expired could acknowledge a task that another worker has since leased. Fixing this requires a per-lease token.
- **No retry limit, backoff, or dead-letter queue yet.** A task that always fails is redelivered indefinitely.
- **Task IDs are nanosecond timestamps,** so two enqueues in the same nanosecond could collide. A Redis counter or UUID would remove the risk.
- **Single Redis node.** There is no replication or persistence tuning in the Compose file.
- **Planned:** retries with exponential backoff and a dead-letter queue, a metrics endpoint, and a small dashboard showing queue depth, throughput, redeliveries, and latency percentiles.
