package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
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

func countIPs(cidr string) int {
	if !strings.Contains(cidr, "/") {
		return 1
	}
	parts := strings.Split(cidr, "/")
	if len(parts) != 2 {
		return 1
	}
	mask, _ := strconv.Atoi(parts[1])
	return int(math.Pow(2, float64(32-mask)))
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
		// A. Fetch Batch Size from Redis (fallback to 100k)
		batchSize := 100000
		if val, err := rdb.Get(ctx, "config:worker_batch_size").Result(); err == nil {
			if n, err := strconv.Atoi(val); err == nil && n > 0 {
				batchSize = n
			}
		}

		// B. Inspect the queue
		allTargets, err := rdb.LRange(ctx, rangesKey, 0, -1).Result()
		if err != nil || len(allTargets) == 0 {
			break
		}

		// C. Calculate total IPs available
		totalIPs := 0
		for _, t := range allTargets {
			totalIPs += countIPs(t)
		}

		// D. Determine how many CIDR items to pick
		var pickCount int
		if totalIPs <= batchSize {
			// Case 1: Small task - pick half of the items (minimum 1)
			pickCount = (len(allTargets) + 1) / 2
		} else {
			// Case 2: Large task - pick items until we reach the batch size limit
			cumulativeIPs := 0
			for i, t := range allTargets {
				cumulativeIPs += countIPs(t)
				pickCount = i + 1
				if cumulativeIPs >= batchSize {
					break
				}
			}
		}

		// E. Pop the batch one by one to keep progress bar smooth
		batch := []string{}
		for i := 0; i < pickCount; i++ {
			t, err := rdb.LPop(ctx, rangesKey).Result()
			if err != nil {
				break
			}
			batch = append(batch, t)
		}

		if len(batch) == 0 {
			break
		}

		batchIPs := 0
		for _, t := range batch {
			batchIPs += countIPs(t)
		}

		fmt.Printf("[Worker %d] Starting batch of %d CIDR(s) (~%d IPs)\n", *workerID, len(batch), batchIPs)

		// F. Process the batch (In a real implementation, you might pass all targets to one masscan call)
		for _, target := range batch {
			fmt.Printf("[Worker %d] Scanning: %s\n", *workerID, target)

			// --- STUB: Actual scan logic ---
			// If you were using masscan here, you could join the batch targets.
			time.Sleep(3 * time.Second)
			// -------------------------------

			fmt.Printf("[Worker %d] Finished: %s\n", *workerID, target)
		}

		fmt.Printf("[Worker %d] Batch complete.\n", *workerID)
	}

	fmt.Printf("[Worker %d] All assigned ranges complete.\n", *workerID)
}
