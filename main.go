package main

import (
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

func main() {

}
