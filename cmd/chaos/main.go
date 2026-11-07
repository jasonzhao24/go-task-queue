package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"taskqueue/queue"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	// Connect over the internal Docker Linux network
	q, err := queue.NewRedisQueue(ctx, "redis:6379")
	if err != nil {
		log.Fatalf("Failed to initialize queue: %v", err)
	}

	// Wipe any leftover tasks from previous runs so counts are exact
	if err := q.Client.FlushDB(ctx).Err(); err != nil {
		log.Fatalf("Failed to flush Redis DB: %v", err)
	}

	totalTasks := 3000000
	numWorkers := 256

	var processedTasks int32
	var simulatedCrashes int32

	// Thread-local slices to avoid Mutex contention
	workerLatencies := make([][]int64, numWorkers+1)

	fmt.Printf("--- Starting Chaos Test & Latency Profiling ---\n")
	fmt.Printf("Pre-loading %d tasks via Batch Pipeline...\n", totalTasks)

	// 1. Pre-load tasks in batches of 50,000
	pipe := q.Client.Pipeline()
	for i := 0; i < totalTasks; i++ {
		taskID := fmt.Sprintf("%d_%d", time.Now().UnixNano(), i)
		pipe.HSet(ctx, q.PayloadKey, taskID, []byte(fmt.Sprintf("chaos_task_%d", i)))
		pipe.ZAdd(ctx, q.QueueKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: taskID})

		if (i+1)%50000 == 0 {
			_, err := pipe.Exec(ctx)
			if err != nil {
				log.Fatalf("Pipeline exec failed: %v", err)
			}
			fmt.Printf("Successfully batched %d tasks...\n", i+1)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Fatalf("Final pipeline exec failed: %v", err)
	}
	fmt.Println("Pre-load complete. Unleashing 256 Chaos Workers...")

	var wg sync.WaitGroup
	startProcess := time.Now()

	// 2. 256 concurrent workers
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerName := fmt.Sprintf("Worker-%d", workerID)

			for {
				if atomic.LoadInt32(&processedTasks) >= int32(totalTasks) {
					break
				}

				taskStartTime := time.Now()

				task, err := q.Dequeue(ctx, workerName)
				if err != nil || task == nil {
					time.Sleep(1 * time.Millisecond)
					continue
				}

				// CHAOS INJECTION: 16.6% chance of simulated worker failure
				if rand.Float32() < 0.166 {
					atomic.AddInt32(&simulatedCrashes, 1)
					continue
				}

				// Normal execution: Acknowledge the task
				err = q.Acknowledge(ctx, task.ID)
				if err == nil {
					latency := time.Since(taskStartTime).Milliseconds()
					workerLatencies[workerID] = append(workerLatencies[workerID], latency)
					atomic.AddInt32(&processedTasks, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	processDuration := time.Since(startProcess)
	tps := float64(totalTasks) / processDuration.Seconds()

	// 3. Merge thread-local latencies and calculate p99
	var allLatencies []int64
	for _, lat := range workerLatencies {
		allLatencies = append(allLatencies, lat...)
	}
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })

	p99Index := int(float64(len(allLatencies)) * 0.99)
	var p99Latency int64
	if len(allLatencies) > 0 {
		p99Latency = allLatencies[p99Index]
	}

	remainingQueue, err := q.Client.ZCard(ctx, q.QueueKey).Result()
	if err != nil {
		log.Fatalf("ZCard check failed: %v", err)
	}
	remainingPayloads, err := q.Client.HLen(ctx, q.PayloadKey).Result()
	if err != nil {
		log.Fatalf("HLen check failed: %v", err)
	}

	fmt.Printf("\n--- Chaos Test Results ---\n")
	fmt.Printf("Total Tasks Enqueued:       %d\n", totalTasks)
	fmt.Printf("Unique Tasks Acknowledged:  %d\n", processedTasks)
	fmt.Printf("Simulated Worker Failures:  %d\n", simulatedCrashes)
	fmt.Printf("Remaining in ZSET (ZCARD):  %d\n", remainingQueue)
	fmt.Printf("Remaining in Hash (HLEN):   %d\n", remainingPayloads)
	fmt.Printf("Queue Throughput:           %.2f tasks/sec\n", tps)
	fmt.Printf("p99 Dequeue-to-Ack Latency: %d ms\n", p99Latency)
}
