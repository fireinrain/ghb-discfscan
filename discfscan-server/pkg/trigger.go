package pkg

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var rctx = context.Background()

type GitHubDispatcher struct {
	Store *Store
	Ref   string // e.g., "main"
}

func NewGitHubDispatcher(store *Store) *GitHubDispatcher {
	return &GitHubDispatcher{
		Store: store,
		Ref:   "main",
	}
}

// WorkerTask is the JSON payload stored in Redis per worker.
type WorkerTask struct {
	TaskType  string `json:"task_type"`
	Target    string `json:"target"` // IP range, e.g. "1.0.0.0-1.0.3.255"
	Port      string `json:"port"`
	Speedtest string `json:"speedtest"`
	TaskID    uint   `json:"task_id"`
}

// NewRedisClient builds a Redis client from the Store config.
func (d *GitHubDispatcher) NewRedisClient() (*redis.Client, error) {
	host := d.Store.GetConfig("redis_host")
	port := d.Store.GetConfig("redis_port")
	pass := d.Store.GetConfig("redis_pass")

	if host == "" {
		return nil, fmt.Errorf("redis_host not configured")
	}
	if port == "" {
		port = "6379"
	}
	return redis.NewClient(&redis.Options{
		Addr:     host + ":" + port,
		Password: pass,
		DB:       0,
	}), nil
}

// TriggerAction pushes per-worker tasks to Redis, then dispatches the GitHub Action.
func (d *GitHubDispatcher) TriggerAction(taskType, target, port, speedtest, workersOverride string, taskID uint) error {
	ghToken := d.Store.GetConfig("github_token")
	repo := d.Store.GetConfig("github_repo")
	if ghToken == "" || repo == "" {
		return fmt.Errorf("GitHub Token and Repo must be configured in the admin settings first")
	}

	// Apply defaults from config if not overridden by caller
	if port == "" {
		port = d.Store.GetConfig("default_ports")
	}
	if port == "" {
		port = "443,2053,2083,2087,2096,8443"
	}
	if speedtest == "" {
		speedtest = d.Store.GetConfig("default_speedtest")
	}
	if speedtest == "" {
		speedtest = "false"
	}
	if taskType == "" {
		taskType = "scan"
	}

	// Determine default worker cap from config
	defaultWorkerCount := 10
	if ws := d.Store.GetConfig("github_workers"); ws != "" {
		if n, err := strconv.Atoi(ws); err == nil && n > 0 {
			defaultWorkerCount = n
		}
	}

	// Count total IPs across all targets
	targets := parseCIDRList(target)
	totalIPs := countTotalIPs(targets)

	// Decide actual worker count based on IP volume
	actualWorkers := decideWorkerCount(totalIPs, defaultWorkerCount)
	if workersOverride != "" {
		// caller explicitly set a workers count/array — use it as the cap
		trimmed := strings.TrimSpace(workersOverride)
		if !strings.HasPrefix(trimmed, "[") {
			if n, err := strconv.Atoi(trimmed); err == nil && n > 0 {
				actualWorkers = n
			}
		}
	}

	// Split targets into actualWorkers sub-ranges
	ranges := splitIntoRanges(targets, actualWorkers)

	// Context for Redis
	rctx := context.Background()

	// Push tasks to Redis
	rdb, err := d.NewRedisClient()
	if err != nil {
		return fmt.Errorf("redis connection failed: %w", err)
	}

	for i, tgt := range ranges {
		workerID := i + 1

		// 1. Store worker metadata (info)
		infoTask := WorkerTask{
			TaskType:  taskType,
			Port:      port,
			Speedtest: speedtest,
			TaskID:    taskID,
		}
		infoData, _ := json.Marshal(infoTask)
		infoKey := fmt.Sprintf("task:worker:%d:%d:info", taskID, workerID)
		rdb.Set(rctx, infoKey, string(infoData), 24*time.Hour)

		// 2. Clear old ranges if any (shouldn't happen on fresh dispatch)
		rangesKey := fmt.Sprintf("task:worker:%d:%d:ranges", taskID, workerID)
		rdb.Del(rctx, rangesKey)

		// 3. Push ranges to the list
		// Current logic: splitIntoRanges gives one big string like "1.1.1.0-1.1.1.255".
		// We'll push this as a single chunk for now, but the worker will LPOP it.
		// Future improvement: split the big range into multiple CIDR chunks here.
		if tgt != "" {
			rdb.RPush(rctx, rangesKey, tgt)
		}
	}

	// 4. Store total chunks count for overall progress tracking
	totalChunksKey := fmt.Sprintf("task:total_chunks:%d", taskID)
	rdb.Set(rctx, totalChunksKey, len(ranges), 24*time.Hour)

	// Build workers array string e.g. "[1,2,3,...,N]"
	nums := make([]string, actualWorkers)
	for i := 0; i < actualWorkers; i++ {
		nums[i] = strconv.Itoa(i + 1)
	}
	workersArray := "[" + strings.Join(nums, ",") + "]"

	// Dispatch GitHub Action with ONLY workers array
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/actions/workflows/do-task-worker.yml/dispatches", repo)
	payload := map[string]interface{}{
		"ref": d.Ref,
		"inputs": map[string]string{
			"workers": workersArray,
			"task_id": strconv.FormatUint(uint64(taskID), 10),
		},
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+ghToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to github: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("github api returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ─── Helpers ────────────────────────────────────────────────────────────────

// parseCIDRList splits a comma-separated list of CIDRs/IPs into individual entries.
func parseCIDRList(target string) []string {
	parts := strings.Split(target, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// countTotalIPs totals the number of usable IPs across all targets.
func countTotalIPs(targets []string) int {
	total := 0
	for _, t := range targets {
		if strings.Contains(t, "/") {
			_, ipNet, err := net.ParseCIDR(t)
			if err != nil {
				total++
				continue
			}
			ones, bits := ipNet.Mask.Size()
			total += int(math.Pow(2, float64(bits-ones)))
		} else {
			total++ // single IP
		}
	}
	return total
}

// decideWorkerCount returns the number of workers based on total IP count.
func decideWorkerCount(totalIPs, defaultMax int) int {
	switch {
	case totalIPs > 100000:
		return defaultMax
	case totalIPs >= 50000:
		return min(10, defaultMax)
	case totalIPs >= 10000:
		return min(5, defaultMax)
	default:
		return 1
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// splitIntoRanges divides the combined IP space of all targets into n equal ranges.
// Returns a slice of range strings like "1.0.0.0-1.0.3.255".
func splitIntoRanges(targets []string, n int) []string {
	// Collect all IP uint32 values from all targets
	type ipRange struct{ start, end uint32 }
	var allRanges []ipRange
	totalIPs := 0

	for _, t := range targets {
		if strings.Contains(t, "/") {
			_, ipNet, err := net.ParseCIDR(t)
			if err != nil {
				continue
			}
			start := ipToUint32(ipNet.IP.Mask(ipNet.Mask))
			ones, bits := ipNet.Mask.Size()
			count := int(math.Pow(2, float64(bits-ones)))
			end := start + uint32(count) - 1
			allRanges = append(allRanges, ipRange{start, end})
			totalIPs += count
		} else {
			ip := net.ParseIP(t).To4()
			if ip == nil {
				continue
			}
			u := ipToUint32(ip)
			allRanges = append(allRanges, ipRange{u, u})
			totalIPs++
		}
	}

	if len(allRanges) == 0 {
		// Fallback: return the original target as-is for worker 1
		result := make([]string, n)
		for i := range result {
			result[i] = strings.Join(targets, ",")
		}
		return result
	}

	if n == 1 {
		return []string{rangeToString(allRanges[0].start, allRanges[0].end)}
	}

	chunkSize := totalIPs / n
	if chunkSize == 0 {
		chunkSize = 1
	}

	var result []string
	remaining := 0
	ri := 0
	cur := allRanges[0].start

	for workerIdx := 0; workerIdx < n && ri < len(allRanges); workerIdx++ {
		need := chunkSize
		if workerIdx == n-1 {
			// Last worker takes the rest
			need = math.MaxInt32
		}

		chunkStart := cur
		filled := 0

		for filled < need && ri < len(allRanges) {
			avail := int(allRanges[ri].end-cur) + 1
			if avail <= 0 {
				ri++
				if ri < len(allRanges) {
					cur = allRanges[ri].start
				}
				remaining = 0
				continue
			}

			take := need - filled
			if take > avail {
				take = avail
			}
			filled += take
			if take == avail {
				cur = allRanges[ri].end
				ri++
				if ri < len(allRanges) {
					cur = allRanges[ri].start
				}
				remaining = 0
			} else {
				cur += uint32(take)
				remaining = avail - take
				_ = remaining
			}
		}

		chunkEnd := cur - 1
		if ri < len(allRanges) && cur > allRanges[ri].start {
			chunkEnd = cur - 1
		} else if ri > 0 {
			chunkEnd = allRanges[ri-1].end
		}

		result = append(result, rangeToString(chunkStart, chunkEnd))
	}

	// Pad with empty entries if fewer ranges produced than workers requested
	for len(result) < n {
		result = append(result, result[len(result)-1])
	}

	return result
}

func ipToUint32(ip net.IP) uint32 {
	ip = ip.To4()
	return binary.BigEndian.Uint32(ip)
}

func uint32ToIP(n uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, n)
	return ip
}

func rangeToString(start, end uint32) string {
	if start == end {
		return uint32ToIP(start).String()
	}
	return uint32ToIP(start).String() + "-" + uint32ToIP(end).String()
}
