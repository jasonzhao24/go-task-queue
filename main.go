package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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
	CreatedAt  int64
}

type EnqueueRequest struct {
	Payload string `json:"payload"`
}

type DequeueRequest struct {
	WorkerID string `json:"worker_id"`
}

type AcknowledgeRequest struct {
	TaskID string `json:"task_id"`
}

func main() {
	ctx := context.Background()

	// Initialize the DynamoDB Queue
	queue, err := NewDynamoQueue(ctx, "Tasks")
	if err != nil {
		log.Fatalf("Failed to initialize DynamoDB client: %v", err)
	}

	queue.CreateTable(ctx)
	go queue.StartReaper(ctx)

	http.HandleFunc("/enqueue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req EnqueueRequest
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &req)

		id, err := queue.Enqueue(ctx, []byte(req.Payload))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"task_id": id})
	})

	http.HandleFunc("/dequeue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		workerID := r.URL.Query().Get("worker_id")
		if workerID == "" {
			http.Error(w, "worker_id is required", http.StatusBadRequest)
			return
		}

		task, err := queue.Dequeue(ctx, workerID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if task == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(task)
	})

	http.HandleFunc("/acknowledge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req AcknowledgeRequest
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &req)

		err := queue.Acknowledge(ctx, req.TaskID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
