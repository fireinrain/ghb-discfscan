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

	"github.com/labstack/gommon/log"
)

var rctx = context.Background()

type GitHubDispatcher struct {
	Store *Store
	Ref   string // e.g., "main"
}

func NewGitHubDispatcher(store *Store) *GitHubDispatcher {
	return &GitHubDispatcher{
		Store: store,
		Ref:   "master",
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
		speedtest = "true"
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
	targets, err := ParseCIDRList(target)
	if err != nil {
		log.Errorf("[Dispatcher] Target parsing failed (Task %d): %v", taskID, err)
		return fmt.Errorf("failed to parse targets: %w", err)
	}
	totalIPs := CountTotalIPs(targets)

	// Record the total IPs in the database (redundant but safe)
	_ = d.Store.SetTaskIPCount(taskID, totalIPs)

	// Decide actual worker count based on IP volume (unless overridden)
	actualWorkers := DecideWorkerCount(totalIPs, defaultWorkerCount)
	if workersOverride != "" {
		trimmed := strings.TrimSpace(workersOverride)
		if n, err := strconv.Atoi(trimmed); err == nil && n > 0 {
			actualWorkers = n
		}
	}

	// Record final worker count in database (redundant but safe)
	_ = d.Store.SetTaskWorkersCount(taskID, actualWorkers)

	// Split targets into actualWorkers CIDR buckets
	buckets := splitIntoCIDRBuckets(targets, actualWorkers)

	// Context for Redis
	rctx := context.Background()

	// Push tasks to Redis
	rdb, err := d.NewRedisClient()
	if err != nil {
		log.Errorf("[Dispatcher] Redis connection failed for task %d: %v", taskID, err)
		return fmt.Errorf("redis connection failed: %w", err)
	}

	totalChunks := 0
	for i, cidrs := range buckets {
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

		// 2. Clear old ranges if any
		rangesKey := fmt.Sprintf("task:worker:%d:%d:ranges", taskID, workerID)
		rdb.Del(rctx, rangesKey)

		// 3. Push CIDRs to the list
		if len(cidrs) > 0 {
			for _, c := range cidrs {
				rdb.RPush(rctx, rangesKey, c)
				totalChunks++
			}
		}
	}

	// 4. Store total chunks count for overall progress tracking
	totalChunksKey := fmt.Sprintf("task:total_chunks:%d", taskID)
	rdb.Set(rctx, totalChunksKey, totalChunks, 24*time.Hour)

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
		log.Errorf("[Dispatcher] GitHub API failure (Task %d): status %d, body: %s", taskID, resp.StatusCode, string(body))
		return fmt.Errorf("github api returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Infof("[Dispatcher] Task %d successfully triggered with %d workers", taskID, actualWorkers)
	return nil
}

// ─── Helpers ────────────────────────────────────────────────────────────────

// resolveASN fetches IPv4 announced prefixes for a given AS number (e.g. AS13335).
func resolveASN(asn string) ([]string, error) {
	// Clean the prefix
	asn = strings.ToUpper(strings.TrimPrefix(strings.TrimSpace(asn), "AS"))
	if asn == "" {
		return nil, fmt.Errorf("invalid ASN format")
	}

	url := fmt.Sprintf("https://stat.ripe.net/data/announced-prefixes/data.json?resource=AS%s", asn)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("RIPE Stat API returned status %d", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Prefixes []struct {
				Prefix string `json:"prefix"`
			} `json:"prefixes"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var cidrs []string
	for _, p := range result.Data.Prefixes {
		// Only keep IPv4 for now as the scanner logic is IPv4-centric
		if strings.Contains(p.Prefix, ":") {
			continue
		}
		cidrs = append(cidrs, p.Prefix)
	}
	return cidrs, nil
}

func ParseCIDRList(target string) ([]string, error) {
	// Replace newlines with commas then split
	normalized := strings.ReplaceAll(target, "\n", ",")
	parts := strings.Split(normalized, ",")

	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		// Handle AS Numbers (AS13335, as13335, 13335? No, let's stick to AS prefix)
		if strings.HasPrefix(strings.ToUpper(p), "AS") {
			prefixes, err := resolveASN(p)
			if err != nil {
				// We log but continue for other targets? Or fail? Let's fail for integrity.
				return nil, fmt.Errorf("failed to resolve %s: %w", p, err)
			}
			out = append(out, prefixes...)
			continue
		}

		// Handle existing CIDRs or raw IPs
		if strings.Contains(p, "/") || strings.Contains(p, "-") {
			out = append(out, p)
		} else {
			// If it's a valid IP, turn into CIDR /32
			if ip := net.ParseIP(p); ip != nil {
				out = append(out, p+"/32")
			} else {
				// Could be some other format or invalid, keep as is for countTotalIPs to figure out
				out = append(out, p)
			}
		}
	}
	return out, nil
}

func CountTotalIPs(targets []string) int {
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

func DecideWorkerCount(totalIPs, defaultMax int) int {
	switch {
	case totalIPs > 100000:
		return defaultMax
	case totalIPs >= 50000:
		return 10
	case totalIPs >= 10000:
		return 5
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

// splitIntoCIDRBuckets divides the target space into n buckets of CIDRs.
func splitIntoCIDRBuckets(targets []string, n int) [][]string {
	if n <= 1 {
		return [][]string{targets}
	}

	// 1. Resolve all targets into a list of base CIDRs
	var allCIDRs []string
	totalIPs := 0
	for _, t := range targets {
		if strings.Contains(t, "-") {
			// Convert "start-end" to CIDRs (fallback if needed)
			allCIDRs = append(allCIDRs, t) // masscan can handle ranges, but user wants CIDRs
			// For now, let's assume targets are already normalized as CIDRs or IPs by parseCIDRList
		} else if strings.Contains(t, "/") {
			allCIDRs = append(allCIDRs, t)
			totalIPs += CountTotalIPs([]string{t})
		} else {
			allCIDRs = append(allCIDRs, t+"/32")
			totalIPs += 1
		}
	}

	if totalIPs == 0 {
		return make([][]string, n)
	}

	avgPerWorker := totalIPs / n
	if avgPerWorker == 0 {
		avgPerWorker = 1
	}

	buckets := make([][]string, n)
	currentWorker := 0
	currentQuota := avgPerWorker

	// Simple bucket filler
	for _, c := range allCIDRs {
		count := CountTotalIPs([]string{c})

		// While this CIDR is too big for the current worker's quota, split it
		for count > currentQuota && currentWorker < n-1 {
			// Split CIDR into two halves
			half1, half2, err := splitCIDR(c)
			if err != nil {
				// Cannot split further (e.g. /32), just give it to the worker
				buckets[currentWorker] = append(buckets[currentWorker], c)
				currentWorker++
				currentQuota = avgPerWorker
				goto nextCIDR
			}

			// Add first half to current worker and move to next
			buckets[currentWorker] = append(buckets[currentWorker], half1)
			c = half2
			count = CountTotalIPs([]string{c})
			currentWorker++
			currentQuota = avgPerWorker
		}

		// Add remaining part to current worker
		buckets[currentWorker] = append(buckets[currentWorker], c)
		currentQuota -= count
		if currentQuota <= 0 && currentWorker < n-1 {
			currentWorker++
			currentQuota = avgPerWorker
		}
	nextCIDR:
	}

	return buckets
}

// splitCIDR splits a CIDR like /24 into two /25s.
func splitCIDR(cidr string) (string, string, error) {
	ip, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", "", err
	}
	ones, _ := ipNet.Mask.Size()
	if ones >= 32 {
		return "", "", fmt.Errorf("cannot split /32")
	}

	// New mask is one bit larger
	newOnes := ones + 1

	// First half is the same start IP
	h1 := fmt.Sprintf("%s/%d", ip.String(), newOnes)

	// Second half is start IP + (2 ^ (32 - newOnes))
	delta := uint32(math.Pow(2, float64(32-newOnes)))
	u32 := ipToUint32(ip)
	h2 := fmt.Sprintf("%s/%d", uint32ToIP(u32+delta).String(), newOnes)

	return h1, h2, nil
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
