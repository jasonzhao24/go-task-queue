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
	q.mu.Lock()
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
func main() {

}
