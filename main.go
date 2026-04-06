package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type WorkerTaskInfo struct {
	TaskType  string `json:"task_type"`
	Port      string `json:"port"`
	Speedtest string `json:"speedtest"`
	TaskID    uint   `json:"task_id"`
}

func startHeartbeat(rdb *redis.Client, taskID uint, workerID int) {
	key := fmt.Sprintf("task:worker:%d:%d:heartbeat", taskID, workerID)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			rdb.Set(ctx, key, time.Now().Unix(), 5*time.Minute)
			<-ticker.C
		}
	}()
}

func main() {
	workerID := flag.Int("worker", 0, "worker_id assigned by GitHub Actions matrix")
	taskIDFlag := flag.Uint("task", 0, "task_id from the database")
	flag.Parse()

	if *workerID == 0 || *taskIDFlag == 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --worker and --task flags are required")
		os.Exit(1)
	}

	host := os.Getenv("REDIS_HOST")
	port := os.Getenv("REDIS_PORT")
	pass := os.Getenv("REDIS_PASS")

	if host == "" {
		host = "127.0.0.1" // Local debug fallback
	}
	if port == "" {
		port = "6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     host + ":" + port,
		Password: pass,
		DB:       0,
	})

	// 1. Fetch Task Info
	infoKey := fmt.Sprintf("task:worker:%d:%d:info", *taskIDFlag, *workerID)
	val, err := rdb.Get(ctx, infoKey).Result()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error fetching task info: %v\n", err)
		os.Exit(1)
	}

	var info WorkerTaskInfo
	if err := json.Unmarshal([]byte(val), &info); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse task info: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("==================================================")
	fmt.Printf(" [WORKER CONFIG] Worker ID: %d\n", *workerID)
	fmt.Printf(" [WORKER CONFIG] Task ID:   %d\n", *taskIDFlag)
	fmt.Printf(" [TASK PARAMETERS]\n")
	fmt.Printf("   Task Type:  %s\n", info.TaskType)
	fmt.Printf("   Ports:      %s\n", info.Port)
	fmt.Printf("   Speedtest:  %s\n", info.Speedtest)
	fmt.Println("==================================================")

	// 2. Start Heartbeat
	startHeartbeat(rdb, *taskIDFlag, *workerID)

	// 3. Process Ranges Loop
	rangesKey := fmt.Sprintf("task:worker:%d:%d:ranges", *taskIDFlag, *workerID)

	for {
		// Fetch next CIDR range from list
		target, err := rdb.LPop(ctx, rangesKey).Result()
		if err == redis.Nil {
			break // No more work
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Redis error pulling range: %v\n", err)
			break
		}

		fmt.Printf("[Worker %d] Scanning: %s\n", *workerID, target)

		// --- STUB: Actual scan logic ---
		time.Sleep(3 * time.Second)
		// -------------------------------

		fmt.Printf("[Worker %d] Finished: %s\n", *workerID, target)
	}

	fmt.Printf("[Worker %d] All assigned ranges complete.\n", *workerID)
}
