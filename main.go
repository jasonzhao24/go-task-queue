package main

import (
	"fmt"
	"sync"
	"time"
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
}

type MemoryQueue struct {
	mu    sync.Mutex
	tasks map[string]*Task
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		tasks: make(map[string]*Task),
	}
}

// # adds task to queue and return ID
func (q *MemoryQueue) Enqueue(payload []byte) string {
	q.mu.Lock() // Locks the mutex so no other works can take the task
	// makes the mutex unlock when the function finishes
	defer q.mu.Unlock()

	id := fmt.Sprintf("%d", time.Now().UnixNano())

	newTask := &Task{
		ID:         id,
		Payload:    payload,
		State:      Available,
		RetryCount: 0,
	}
	// Saves to map
	q.tasks[id] = newTask

	return id
}

func (q *MemoryQueue) Dequeue(workerID string) *Task {
	q.mu.Lock()
	defer q.mu.Unlock() // same as enqueue func

	for _, task := range q.tasks {
		if task.State == Available {
			task.State = Leased      // Mark as leased so no worker is able to take it
			task.WorkerID = workerID // Keep track of which worker claimed it
			task.VisibleAt = time.Now().Add(5 * time.Second)
			return task
		}
	}
	return nil
}

func (q *MemoryQueue) Acknowledge(taskID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Deletes key from the map and removes successfully completed task from queue
	delete(q.tasks, taskID)
}
func (q *MemoryQueue) StartReaper() {
	for { // Runs as long as the program is running
		time.Sleep(1 * time.Second)
		q.mu.Lock()
		for _, task := range q.tasks {
			if task.State == Leased && time.Now().After(task.VisibleAt) {
				task.State = Available // Once the time is past the visibility time, we make the task available again
				task.WorkerID = ""     // Remove the existing worker on the task
				task.RetryCount++
				fmt.Printf("[Reaper] Lease expired for task %s. Resetting to Available. (Retries: %d)\n", task.ID, task.RetryCount)

			}
		}
		q.mu.Unlock()
	}
}
func main() { // Testing Functions
	
	queue := NewMemoryQueue()

	// The 'go' keyword spins this function off into a lightweight background thread
	go queue.StartReaper()

	// 1. Enqueue
	taskID := queue.Enqueue([]byte("process_payment_123"))
	fmt.Printf("Enqueued: %s\n", taskID)

	// 2. Worker-A leases the task but CRASHES (never calls Acknowledge)
	task1 := queue.Dequeue("worker-A")
	fmt.Printf("Worker-A leased task: %s\n", task1.ID)
	fmt.Println("Worker-A crashed! Waiting for Reaper to step in...")

	// 3. Wait for 6 seconds (our Dequeue timeout was 5 seconds)
	time.Sleep(6 * time.Second)

	// 4. Worker-B checks the queue
	task2 := queue.Dequeue("worker-B")
	if task2 != nil {
		fmt.Printf("Worker-B successfully leased the recovered task: %s\n", task2.ID)
	}
}
