package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

var rdb = redis.NewClient(&redis.Options{
	Addr: "你的Redis地址",
})

type Task struct {
	ID     string
	Type   string
	Target string
}

func main() {
	//for {
	//	taskStr, err := fetchTask()
	//	if err != nil || taskStr == "" {
	//		fmt.Println("no task, exit")
	//		return
	//	}
	//
	//	var task Task
	//	json.Unmarshal([]byte(taskStr), &task)
	//
	//	process(task)
	//}
	workerID := flag.String("worker", "0", "worker_id")
	taskType := flag.String("task_type", "", "task_type")
	target := flag.String("target", "", "target")
	port := flag.String("port", "443", "port")
	speedTest := flag.Bool("speedtest", false, "speedtest")

	flag.Parse()
	fmt.Println("worker:", *workerID)
	fmt.Println("task:", *taskType)
	fmt.Println("target:", *target)
	fmt.Println("port:", *port)
	fmt.Println("speedTest:", *speedTest)
}

func fetchTask() (string, error) {
	res, err := rdb.BRPop(ctx, 5*time.Second, "queue:task").Result()
	if err != nil || len(res) < 2 {
		return "", err
	}
	return res[1], nil
}

func process(t Task) {
	if !acquireLock(t.ID) {
		fmt.Println("task locked, skip:", t.ID)
		return
	}

	defer rdb.Del(ctx, "lock:task:"+t.ID)

	key := "task:" + t.ID
	rdb.HSet(ctx, key, "status", "running")

	for i := 0; i <= 100; i += 10 {
		time.Sleep(1 * time.Second)

		rdb.HSet(ctx, key,
			"progress", fmt.Sprintf("%d%%", i),
			"updated_at", time.Now().Unix(),
		)
	}

	rdb.HSet(ctx, key, "status", "done")
}

func acquireLock(taskID string) bool {
	key := "lock:task:" + taskID

	ok, _ := rdb.SetNX(ctx, key, "1", 10*time.Minute).Result()
	return ok
}
