# Go Distributed Task Queue

A highly concurrent, fault-tolerant distributed task queue built in Go and backed by Amazon DynamoDB. Designed to handle lock contention, worker node crashes, and atomic state transitions using conditional expressions and background lease expiration.

## 🏗 System Architecture

This project treats DynamoDB as a persistent queue, leveraging native NoSQL features to guarantee atomic task leasing and chronological processing across distributed worker nodes.

*   **Global Secondary Index (GSI):** Tasks are queried via a GSI (`State-CreatedAt-Index`) which sorts available tasks by timestamp, ensuring FIFO-style retrieval without full table scans.
*   **Atomic Conditional Leases:** Workers claim tasks using DynamoDB `UpdateItem` with a `ConditionExpression` (`#state = :available`). This guarantees that if 100 concurrent workers request the same task, only one database transaction succeeds. The rest safely loop to the next available task.
*   **Visibility Timeouts:** When a task is leased, it receives a `VisibleAt` timestamp (e.g., `Now() + 5s`). If the worker crashes before acknowledging the task, the task remains locked until this timestamp expires.
*   **The Reaper (Fault Tolerance):** A background goroutine persistently scans the GSI for leased tasks that have exceeded their `VisibleAt` threshold. It automatically resets these orphaned tasks back to the `Available` state, increments their `RetryCount`, and strips the dead worker's ID.

## 🛠 Tech Stack
*   **Language:** Go (1.24)
*   **Database:** Amazon DynamoDB (Local)
*   **Cloud SDK:** AWS SDK for Go v2
*   **Containerization:** Docker & Docker Compose
*   **API:** Go native `net/http` REST server

## 🚀 Quick Start

Ensure Docker is installed, then spin up the entire system (the DynamoDB container and the Go API) with a single command:

```bash
docker-compose up --build
```
*The API will be available on `http://localhost:8080` and connected to the DynamoDB instance on port 8000.*

## 📡 API Reference

### 1. Enqueue a Task
Submit a new job to the queue.
**POST** `/enqueue`
```json
// Request Body
{
  "payload": "encode_video_720p"
}
```
**cURL Example:**
```bash
curl -X POST http://localhost:8080/enqueue -H "Content-Type: application/json" -d "{\"payload\": \"encode_video_720p\"}"
```

### 2. Dequeue a Task
Lease the oldest available job. Requires a `worker_id` query parameter to track lock ownership.
**GET** `/dequeue?worker_id={worker_id}`

**cURL Example:**
```bash
curl "http://localhost:8080/dequeue?worker_id=worker-A"
```
**Response:**
```json
{
  "ID": "1793997012493456300",
  "Payload": "cHJvY2Vzc192aWRlb185OTk=",
  "State": 1,
  "WorkerID": "worker-A",
  "VisibleAt": "2026-11-06T21:04:32.424141893Z",
  "RetryCount": 0,
  "CreatedAt": 1793997012
}
```
*(Note: Payloads are Base64 encoded by the AWS SDK when serialized)*

### 3. Acknowledge a Task
Mark a task as complete. This permanently deletes the item from DynamoDB, preventing the Reaper from resetting it.
**POST** `/acknowledge`
```json
// Request Body
{
  "task_id": "1793997012493456300"
}
```
**cURL Example:**
```bash
curl -X POST http://localhost:8080/acknowledge -H "Content-Type: application/json" -d "{\"task_id\": \"1793997012493456300\"}"
```

## 📊 Performance & Load Testing
This queue architecture was benchmarked utilizing atomic counters and load-testing scripts to profile distributed system throughput. Test environments utilized 16 concurrent Go worker routines simultaneously querying the GSI, confirming zero double-leases and immediate lock contention resolution.
