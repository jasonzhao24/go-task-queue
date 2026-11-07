package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"taskqueue/queue"
)

type EnqueueRequest struct {
	Payload string `json:"payload"`
}

type AcknowledgeRequest struct {
	TaskID string `json:"task_id"`
}

func main() {
	ctx := context.Background()

	// Use the queue package prefix here:
	q, err := queue.NewRedisQueue(ctx, "redis:6379")
	if err != nil {
		log.Fatalf("Failed to initialize Redis client: %v", err)
	}

	http.HandleFunc("/enqueue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req EnqueueRequest
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &req)

		task, err := q.Enqueue(ctx, []byte(req.Payload))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"task_id": task.ID})
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

		task, err := q.Dequeue(ctx, workerID)
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

		err := q.Acknowledge(ctx, req.TaskID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
