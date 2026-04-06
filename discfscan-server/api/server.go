package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"discfscan-server/pkg"
	"discfscan-server/tgbot"
)

type APIServer struct {
	echo         *echo.Echo
	store        *pkg.Store
	dispatcher   *pkg.GitHubDispatcher
	jwtSecret    []byte
	tgBotManager *tgbot.TGBotManager
}

func NewAPIServer(store *pkg.Store, dispatcher *pkg.GitHubDispatcher, tgManager *tgbot.TGBotManager) *APIServer {
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	// CORS for UI
	e.Use(middleware.CORS())

	server := &APIServer{
		echo:         e,
		store:        store,
		dispatcher:   dispatcher,
		jwtSecret:    []byte("super-secret-key-discfscan-2026"),
		tgBotManager: tgManager,
	}

	server.setupRoutes()

	return server
}

func (s *APIServer) setupRoutes() {
	// Serve static UI
	s.echo.Static("/ui", "ui")
	s.echo.File("/dashboard", "ui/index.html")
	// Redirect root to dashboard
	s.echo.GET("/", func(c echo.Context) error {
		return c.Redirect(http.StatusFound, "/dashboard")
	})

	// Trigger Auth Middleware (for workers/scripts)
	workerAuthMiddleware := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			authHeader := c.Request().Header.Get("Authorization")
			if len(authHeader) < 8 || authHeader[:7] != "Bearer " {
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Missing or invalid token"})
			}
			token := authHeader[7:]

			if !s.store.VerifyToken(token) {
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Invalid token"})
			}

			return next(c)
		}
	}

	// Admin Auth Middleware (for dashboard UI)
	adminAuthMiddleware := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			authHeader := c.Request().Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Missing token"})
			}
			tokenStr := authHeader[7:]

			token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
				return s.jwtSecret, nil
			})

			if err != nil || !token.Valid {
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Invalid token"})
			}

			claims := token.Claims.(jwt.MapClaims)
			c.Set("admin_id", claims["admin_id"])

			return next(c)
		}
	}

	// Auth Endpoints
	authGrp := s.echo.Group("/api/auth")
	authGrp.POST("/login", s.handleLogin)
	authGrp.POST("/change-password", s.handleChangePassword, adminAuthMiddleware)

	// Trigger Action Endpoint (Requires Worker Token)
	s.echo.POST("/api/scan", s.handleTriggerScan, workerAuthMiddleware)

	// Admin Dashboard Endpoints (Requires Admin Token)
	mgr := s.echo.Group("/api/tokens", adminAuthMiddleware)
	mgr.GET("", s.handleListTokens)
	mgr.POST("", s.handleCreateToken)
	mgr.DELETE("/:id", s.handleDeleteToken)

	// Config Management Endpoints (Requires Admin Token)
	cfgMgr := s.echo.Group("/api/config", adminAuthMiddleware)
	cfgMgr.GET("", s.handleGetConfig)
	cfgMgr.POST("", s.handleSetConfig)

	// Telegram Bot Management Endpoints
	tgGrp := s.echo.Group("/api/tg", adminAuthMiddleware)
	tgGrp.GET("/status", s.handleTGStatus)
	tgGrp.POST("/config", s.handleTGConfig)
	tgGrp.POST("/test", s.handleTGTest)
	tgGrp.GET("/rules", s.handleTGListRules)
	tgGrp.POST("/rules", s.handleTGCreateRule)
	tgGrp.DELETE("/rules/:id", s.handleTGDeleteRule)

	// Task Queue Endpoints
	taskGrp := s.echo.Group("/api/tasks", adminAuthMiddleware)
	taskGrp.GET("", s.handleListTasks)
	taskGrp.POST("", s.handleCreateTask)
	taskGrp.DELETE("/:id", s.handleDeleteTask)
	taskGrp.POST("/:id/cancel", s.handleCancelTask)
	taskGrp.POST("/:id/promote", s.handlePromoteTask)
}

// StartDispatcher launches a background goroutine that dispatches the next pending task
// when no task is currently running.
func (s *APIServer) StartDispatcher() {
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.tryDispatchNext()
			s.checkAndRescueStalledTasks()
			s.updateRunningTaskProgress()
		}
	}()
}

func (s *APIServer) updateRunningTaskProgress() {
	task := s.store.GetNextRunningTask()
	if task == nil {
		return
	}

	progress := s.calculateTaskProgress(task.ID)
	status := "running"
	if progress >= 100 {
		status = "completed"
		now := time.Now()
		task.CompletedAt = &now
	}

	// Update task in database
	err := s.store.UpdateTaskProgress(task.ID, status, progress)
	if err == nil && status == "completed" {
		fmt.Printf("[Dispatcher] Task %d completed autonomously. Triggering next.\n", task.ID)
		go s.tryDispatchNext()
	}
}

func (s *APIServer) checkAndRescueStalledTasks() {
	task := s.store.GetNextRunningTask()
	if task == nil {
		return
	}

	// 1. Time Check: Has it been running too long without update?
	// (Check updated_at since every chunk report updates it)
	if time.Since(task.UpdatedAt) < 15*time.Minute {
		return
	}

	// 2. Redis Check: Which workers are actually dead?
	rdb, err := s.dispatcher.NewRedisClient() // Need a public method or internal access
	if err != nil {
		return
	}

	ctx := context.Background()
	var rescueWorkerIDs []string

	// We need to know how many workers were originally assigned.
	// This is stored in task.Workers (string).
	var workerIDs []int
	if strings.HasPrefix(task.Workers, "[") {
		json.Unmarshal([]byte(task.Workers), &workerIDs)
	}

	for _, wid := range workerIDs {
		// Check heartbeat
		hbKey := fmt.Sprintf("task:worker:%d:%d:heartbeat", task.ID, wid)
		lastHB, _ := rdb.Get(ctx, hbKey).Result()

		// If no heartbeat found or it's old
		if lastHB == "" {
			// Check if work remains
			rangeKey := fmt.Sprintf("task:worker:%d:%d:ranges", task.ID, wid)
			rem, _ := rdb.LLen(ctx, rangeKey).Result()
			if rem > 0 {
				rescueWorkerIDs = append(rescueWorkerIDs, fmt.Sprintf("%d", wid))
			}
		}
	}

	if len(rescueWorkerIDs) > 0 {
		workersStr := "[" + strings.Join(rescueWorkerIDs, ",") + "]"
		fmt.Printf("[Dispatcher] Rescuing task %d, re-triggering workers: %s\n", task.ID, workersStr)
		s.dispatcher.TriggerAction(task.TaskType, task.Target, task.Port, task.Speedtest, workersStr, task.ID)
	}
}

func (s *APIServer) tryDispatchNext() {
	if s.store.HasRunningTask() {
		return
	}
	task := s.store.GetNextPendingTask()
	if task == nil {
		return
	}
	if err := s.store.MarkTaskRunning(task.ID); err != nil {
		return
	}
	err := s.dispatcher.TriggerAction(task.TaskType, task.Target, task.Port, task.Speedtest, task.Workers, task.ID)
	if err != nil {
		// Mark as failed so queue doesn't stall
		_ = s.store.UpdateTaskProgress(task.ID, "failed", 0)
	}
}

func (s *APIServer) handleLogin(c echo.Context) error {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	admin, ok := s.store.VerifyAdmin(req.Username, req.Password)
	if !ok {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Invalid credentials"})
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"admin_id": admin.ID,
		"username": admin.Username,
		"exp":      time.Now().Add(time.Hour * 72).Unix(),
	})

	t, _ := token.SignedString(s.jwtSecret)

	return c.JSON(http.StatusOK, map[string]interface{}{
		"token":          t,
		"is_first_login": admin.IsFirstLogin,
	})
}

func (s *APIServer) handleChangePassword(c echo.Context) error {
	var req struct {
		NewPassword string `json:"new_password"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	if len(req.NewPassword) < 5 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Password too short"})
	}

	adminID := uint(c.Get("admin_id").(float64))
	err := s.store.UpdateAdminPassword(adminID, req.NewPassword)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Internal error"})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "success"})
}

func (s *APIServer) handleTriggerScan(c echo.Context) error {
	type ScanRequest struct {
		Label       string `json:"label"`
		Description string `json:"description"`
		TaskType    string `json:"task_type"`
		Target      string `json:"target"`
		Port        string `json:"port"`
		Speedtest   string `json:"speedtest"`
		Workers     string `json:"workers"`
	}

	var req ScanRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}

	// Defaults if missing
	if req.TaskType == "" {
		req.TaskType = "scan"
	}
	if req.Port == "" {
		req.Port = s.store.GetConfig("default_ports")
	}
	if req.Port == "" {
		req.Port = "443,2053,2083,2087,2096,8443"
	}
	if req.Speedtest == "" {
		req.Speedtest = "true"
	}

	if req.Target == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Target is required"})
	}

	// Queue the task instead of triggering immediately
	task, err := s.store.CreateTask(req.Label, req.Description, req.TaskType, req.Target, req.Port, req.Speedtest, req.Workers)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	// Attempt to dispatch next if idle
	go s.tryDispatchNext()

	return c.JSON(http.StatusOK, map[string]interface{}{
		"status":  "success",
		"message": "Scan job queued successfully",
		"task_id": task.ID,
	})
}

// Token Management
func (s *APIServer) handleListTokens(c echo.Context) error {
	tokens, err := s.store.ListTokens()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, tokens)
}

func (s *APIServer) handleCreateToken(c echo.Context) error {
	var req struct {
		Description string `json:"description"`
		ExpireHours int    `json:"expire_hours"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	token, err := s.store.GenerateToken(req.Description, req.ExpireHours)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, token)
}

func (s *APIServer) handleDeleteToken(c echo.Context) error {
	id := c.Param("id")
	// quick string to uint convert using standard logic is missing, let's use fmt Sscanf
	var idUint uint
	fmt.Sscanf(id, "%d", &idUint)

	err := s.store.DeleteToken(idUint)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *APIServer) handleGetConfig(c echo.Context) error {
	token := s.store.GetConfig("github_token")
	repo := s.store.GetConfig("github_repo")
	workers := s.store.GetConfig("github_workers")
	if workers == "" {
		workers = "10"
	}
	// Mask the token if it exists
	if len(token) > 8 {
		token = token[:4] + "********" + token[len(token)-4:]
	} else if len(token) > 0 {
		token = "********"
	}
	return c.JSON(http.StatusOK, map[string]string{
		"github_token":   token,
		"github_repo":    repo,
		"github_workers": workers,
		"redis_host":     s.store.GetConfig("redis_host"),
		"redis_port":     s.store.GetConfig("redis_port"),
		"redis_pass":     s.store.GetConfig("redis_pass"),
		"server_url":     s.store.GetConfig("server_url"),
		"default_ports": func() string {
			if v := s.store.GetConfig("default_ports"); v != "" {
				return v
			}
			return "443,2053,2083,2087,2096,8443"
		}(),
		"default_speedtest": func() string {
			if v := s.store.GetConfig("default_speedtest"); v != "" {
				return v
			}
			return "false"
		}(),
	})
}

func (s *APIServer) handleSetConfig(c echo.Context) error {
	var req map[string]string
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if val, ok := req["github_token"]; ok && val != "" {
		if !strings.Contains(val, "********") {
			s.store.SetConfig("github_token", val)
		}
	}
	if val, ok := req["github_repo"]; ok {
		s.store.SetConfig("github_repo", val)
	}
	if val, ok := req["github_workers"]; ok {
		s.store.SetConfig("github_workers", val)
	}
	if val, ok := req["redis_host"]; ok {
		s.store.SetConfig("redis_host", val)
	}
	if val, ok := req["redis_port"]; ok {
		s.store.SetConfig("redis_port", val)
	}
	if val, ok := req["redis_pass"]; ok {
		s.store.SetConfig("redis_pass", val)
	}
	if val, ok := req["default_ports"]; ok {
		s.store.SetConfig("default_ports", val)
	}
	if val, ok := req["default_speedtest"]; ok {
		s.store.SetConfig("default_speedtest", val)
	}
	if val, ok := req["server_url"]; ok {
		s.store.SetConfig("server_url", val)
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "success"})
}

func (s *APIServer) handleTGConfig(c echo.Context) error {
	var req struct {
		Token string `json:"token"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid format"})
	}

	s.store.SetConfig("tg_token", req.Token)

	if req.Token != "" {
		if err := s.tgBotManager.Start(req.Token); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	} else {
		s.tgBotManager.Stop()
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "success"})
}

func (s *APIServer) handleTGStatus(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"is_running": s.tgBotManager.IsRunning(),
		"token":      s.store.GetConfig("tg_token"),
	})
}

func (s *APIServer) handleTGTest(c echo.Context) error {
	var req struct {
		ChatID int64  `json:"chat_id"`
		Text   string `json:"text"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}
	if err := s.tgBotManager.SendTestMessage(req.ChatID, req.Text); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "success"})
}

func (s *APIServer) handleTGListRules(c echo.Context) error {
	rules, err := s.store.ListTGRules()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, rules)
}

func (s *APIServer) handleTGCreateRule(c echo.Context) error {
	var req struct {
		UserID   int64  `json:"user_id"`
		ChatID   int64  `json:"chat_id"`
		Name     string `json:"name"`
		Commands string `json:"commands"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid payload"})
	}
	rule, err := s.store.CreateTGRule(req.UserID, req.ChatID, req.Name, req.Commands)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, rule)
}

func (s *APIServer) handleTGDeleteRule(c echo.Context) error {
	id := c.Param("id")
	var idUint uint
	fmt.Sscanf(id, "%d", &idUint)
	if err := s.store.DeleteTGRule(idUint); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *APIServer) Start(address string) error {
	return s.echo.Start(address)
}

// ─── Task Queue Handlers ─────────────────────────────────────────────────────

func (s *APIServer) handleListTasks(c echo.Context) error {
	tasks, err := s.store.ListTasks()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, tasks)
}

func (s *APIServer) handleCreateTask(c echo.Context) error {
	var req struct {
		Label       string `json:"label"`
		Description string `json:"description"`
		Target      string `json:"target"`
		Port        string `json:"port"`
		Speedtest   string `json:"speedtest"`
		Workers     string `json:"workers"`
		TaskType    string `json:"task_type"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}
	if req.Target == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Target is required"})
	}
	if req.TaskType == "" {
		req.TaskType = "scan"
	}
	task, err := s.store.CreateTask(req.Label, req.Description, req.TaskType, req.Target, req.Port, req.Speedtest, req.Workers)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	// Immediately try to dispatch if no task is running
	go s.tryDispatchNext()
	return c.JSON(http.StatusOK, task)
}

func (s *APIServer) handleDeleteTask(c echo.Context) error {
	var idUint uint
	fmt.Sscanf(c.Param("id"), "%d", &idUint)
	if err := s.store.DeleteTask(idUint); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *APIServer) handleCancelTask(c echo.Context) error {
	var idUint uint
	fmt.Sscanf(c.Param("id"), "%d", &idUint)
	if err := s.store.CancelTask(idUint); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "cancelled"})
}

func (s *APIServer) handlePromoteTask(c echo.Context) error {
	var idUint uint
	fmt.Sscanf(c.Param("id"), "%d", &idUint)
	if err := s.store.PromoteTask(idUint); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "promoted"})
}

func (s *APIServer) calculateTaskProgress(taskID uint) int {
	rdb, _ := s.dispatcher.NewRedisClient()
	ctx := context.Background()

	// Get total chunks
	totalChunksKey := fmt.Sprintf("task:total_chunks:%d", taskID)
	totalStr, _ := rdb.Get(ctx, totalChunksKey).Result()
	total, _ := strconv.Atoi(totalStr)
	if total == 0 {
		return 0
	}

	// Sum remaining across all workers
	// To find how many workers, we check the SQL record or the Redis keys?
	// Let's just scan for task:worker:<taskID>:*:ranges
	remaining := 0
	keys, _ := rdb.Keys(ctx, fmt.Sprintf("task:worker:%d:*:ranges", taskID)).Result()
	for _, k := range keys {
		rem, _ := rdb.LLen(ctx, k).Result()
		remaining += int(rem)
	}

	progress := 100 - (remaining * 100 / total)
	if progress > 100 {
		progress = 100
	}
	return progress
}
