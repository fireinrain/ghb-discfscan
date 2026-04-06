package pkg

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Token struct {
	ID          uint       `gorm:"primaryKey" json:"id"`
	TokenString string     `gorm:"uniqueIndex;not null" json:"token"`
	Description string     `json:"description"`
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   *time.Time `json:"expires_at"`
}

type Config struct {
	Key   string `gorm:"primaryKey" json:"key"`
	Value string `json:"value"`
}

type Admin struct {
	ID           uint   `gorm:"primaryKey" json:"id"`
	Username     string `gorm:"uniqueIndex;not null" json:"username"`
	PasswordHash string `gorm:"not null" json:"-"`
	IsFirstLogin bool   `gorm:"default:true" json:"is_first_login"`
}

type TGRule struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	UserID    int64     `gorm:"index;not null" json:"user_id"`
	ChatID    int64     `gorm:"index;not null" json:"chat_id"` // 0 means any chat
	Name      string    `json:"name"`
	Commands  string    `json:"commands"` // e.g. "/run,/status" or "*"
	CreatedAt time.Time `json:"created_at"`
}

// ScanTask represents a queued scan job.
type ScanTask struct {
	ID          uint       `gorm:"primaryKey" json:"id"`
	Label       string     `json:"label"`
	Description string     `json:"description"`
	TaskType    string     `gorm:"default:scan" json:"task_type"`
	Target      string     `json:"target"`
	Port        string     `json:"port"`
	Speedtest   string     `gorm:"default:false" json:"speedtest"`
	Workers     string     `json:"workers"`                       // optional override
	Progress    int        `gorm:"default:0" json:"progress"`     // 0-100
	Status      string     `gorm:"default:pending" json:"status"` // pending | running | completed | failed | cancelled
	SortOrder   int        `gorm:"index" json:"sort_order"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
	IPCount     int        `json:"ip_count"`
}

type Store struct {
	db *gorm.DB
}

func initDB() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open("tokens.db"), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	// Auto Migrate the schema
	err = db.AutoMigrate(&Token{}, &Config{}, &Admin{}, &TGRule{}, &ScanTask{})
	if err != nil {
		return nil, err
	}
	return db, nil
}

func NewStoreWithDB(db *gorm.DB) *Store {
	return &Store{db: db}
}

func NewStore() (*Store, error) {
	db, err := initDB()
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}

	if err := s.EnsureDefaultAdmin(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Store) GenerateToken(desc string, expireHours int) (*Token, error) {
	bytes := make([]byte, 16) // 32 chars hex
	if _, err := rand.Read(bytes); err != nil {
		return nil, err
	}

	var expiresAt *time.Time
	if expireHours > 0 {
		t := time.Now().Add(time.Duration(expireHours) * time.Hour)
		expiresAt = &t
	}

	t := &Token{
		TokenString: hex.EncodeToString(bytes),
		Description: desc,
		CreatedAt:   time.Now(),
		ExpiresAt:   expiresAt,
	}
	res := s.db.Create(t)
	return t, res.Error
}

func (s *Store) VerifyToken(tokenString string) bool {
	var count int64
	s.db.Model(&Token{}).
		Where("token_string = ?", tokenString).
		Where("expires_at IS NULL OR expires_at > ?", time.Now()).
		Count(&count)
	return count > 0
}

func (s *Store) ListTokens() ([]Token, error) {
	var tokens []Token
	res := s.db.Find(&tokens)
	return tokens, res.Error
}

func (s *Store) DeleteToken(id uint) error {
	res := s.db.Delete(&Token{}, id)
	return res.Error
}

func (s *Store) GetConfig(key string) string {
	var cfg Config
	res := s.db.First(&cfg, "key = ?", key)
	if res.Error != nil {
		return ""
	}
	return cfg.Value
}

func (s *Store) SetConfig(key, value string) error {
	cfg := Config{Key: key, Value: value}
	// Save will update if primary key exists, or insert if it doesn't
	res := s.db.Save(&cfg)
	return res.Error
}

func (s *Store) EnsureDefaultAdmin() error {
	var count int64
	s.db.Model(&Admin{}).Count(&count)
	if count == 0 {
		hash, err := bcrypt.GenerateFromPassword([]byte("admin"), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		admin := Admin{Username: "admin", PasswordHash: string(hash), IsFirstLogin: true}
		return s.db.Create(&admin).Error
	}
	return nil
}

func (s *Store) VerifyAdmin(username, password string) (*Admin, bool) {
	var admin Admin
	if err := s.db.Where("username = ?", username).First(&admin).Error; err != nil {
		return nil, false
	}
	err := bcrypt.CompareHashAndPassword([]byte(admin.PasswordHash), []byte(password))
	return &admin, err == nil
}

func (s *Store) UpdateAdminPassword(adminID uint, newPassword string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return s.db.Model(&Admin{}).Where("id = ?", adminID).Updates(map[string]interface{}{
		"password_hash":  string(hash),
		"is_first_login": false,
	}).Error
}

func (s *Store) CreateTGRule(userID int64, chatID int64, name string, commands string) (*TGRule, error) {
	rule := &TGRule{
		UserID:    userID,
		ChatID:    chatID,
		Name:      name,
		Commands:  commands,
		CreatedAt: time.Now(),
	}
	res := s.db.Create(rule)
	return rule, res.Error
}

func (s *Store) ListTGRules() ([]TGRule, error) {
	var rules []TGRule
	res := s.db.Find(&rules)
	return rules, res.Error
}

func (s *Store) DeleteTGRule(id uint) error {
	return s.db.Delete(&TGRule{}, id).Error
}

func (s *Store) VerifyTGCommand(userID int64, chatID int64, cmd string) bool {
	var rules []TGRule
	s.db.Where("user_id = ?", userID).Find(&rules)

	for _, rule := range rules {
		if rule.ChatID != 0 && rule.ChatID != chatID {
			continue
		}
		if rule.Commands == "*" {
			return true
		}
		cmds := strings.Split(rule.Commands, ",")
		for _, c := range cmds {
			if strings.TrimSpace(c) == cmd {
				return true
			}
		}
	}
	return false
}

// ─── ScanTask Queue Methods ──────────────────────────────────────────────────

var ErrTaskNotPending = errors.New("task is not in pending state")

// CreateTask adds a new scan task to the end of the queue.
func (s *Store) CreateTask(label, desc, taskType, target, port, speedtest, workers string) (*ScanTask, error) {
	// Find the current max sort_order for pending tasks
	var maxOrder int
	s.db.Model(&ScanTask{}).Select("COALESCE(MAX(sort_order), 0)").Scan(&maxOrder)

	task := &ScanTask{
		Label:       label,
		Description: desc,
		TaskType:    taskType,
		Target:      target,
		Port:        port,
		Speedtest:   speedtest,
		Workers:     workers,
		Status:      "pending",
		SortOrder:   maxOrder + 1,
		CreatedAt:   time.Now(),
	}
	res := s.db.Create(task)
	return task, res.Error
}

// GetNextRunningTask returns the first task that is currently in "running" state.
func (s *Store) GetNextRunningTask() *ScanTask {
	var task ScanTask
	res := s.db.Where("status = ?", "running").Order("updated_at DESC").First(&task)
	if res.Error != nil {
		return nil
	}
	return &task
}

// ListTasksPaged returns tasks for a specific page plus the total count.
func (s *Store) ListTasksPaged(page, pageSize int) ([]ScanTask, int64, error) {
	var tasks []ScanTask
	var total int64

	// Count total first
	s.db.Model(&ScanTask{}).Count(&total)

	// Fetch page (Order by status: running first, then sort_order, then created_at desc for history)
	// Actually let's do: running/pending by sort_order, completed/failed by created_at desc
	res := s.db.Order("CASE WHEN status = 'running' THEN 0 WHEN status = 'pending' THEN 1 ELSE 2 END ASC, created_at DESC").
		Offset((page - 1) * pageSize).
		Limit(pageSize).
		Find(&tasks)

	return tasks, total, res.Error
}

// SetTaskIPCount updates the total number of IPs for a task.
func (s *Store) SetTaskIPCount(id uint, count int) error {
	return s.db.Model(&ScanTask{}).Where("id = ?", id).Update("ip_count", count).Error
}

// DeleteTask removes a task record only if it is not currently running.
func (s *Store) DeleteTask(id uint) error {
	var task ScanTask
	if err := s.db.First(&task, id).Error; err != nil {
		return err
	}
	if task.Status == "running" {
		return fmt.Errorf("cannot delete a running task")
	}
	return s.db.Delete(&ScanTask{}, id).Error
}

// CancelTask marks a pending task as cancelled.
func (s *Store) CancelTask(id uint) error {
	var task ScanTask
	if err := s.db.First(&task, id).Error; err != nil {
		return err
	}
	if task.Status != "pending" {
		return ErrTaskNotPending
	}
	now := time.Now()
	return s.db.Model(&task).Updates(map[string]interface{}{
		"status":       "cancelled",
		"completed_at": &now,
	}).Error
}

// PromoteTask moves a pending task to the front of the queue.
func (s *Store) PromoteTask(id uint) error {
	var task ScanTask
	if err := s.db.First(&task, id).Error; err != nil {
		return err
	}
	if task.Status != "pending" {
		return ErrTaskNotPending
	}
	var minOrder int
	s.db.Model(&ScanTask{}).Where("status = ?", "pending").Select("COALESCE(MIN(sort_order), 0)").Scan(&minOrder)
	return s.db.Model(&task).Update("sort_order", minOrder-1).Error
}

// HasRunningTask returns true if any task is currently in 'running' state.
func (s *Store) HasRunningTask() bool {
	var count int64
	s.db.Model(&ScanTask{}).Where("status = ?", "running").Count(&count)
	return count > 0
}

// GetNextPendingTask returns the next task to dispatch.
func (s *Store) GetNextPendingTask() *ScanTask {
	var task ScanTask
	res := s.db.Where("status = ?", "pending").Order("sort_order ASC, created_at ASC").First(&task)
	if res.Error != nil {
		return nil
	}
	return &task
}

// MarkTaskRunning sets a task to 'running' and records the start time.
func (s *Store) MarkTaskRunning(id uint) error {
	now := time.Now()
	return s.db.Model(&ScanTask{}).Where("id = ?", id).Updates(map[string]interface{}{
		"status":     "running",
		"started_at": &now,
		"progress":   0,
	}).Error
}

// UpdateTaskProgress updates the progress and status of a task (called by workers via API or dispatcher).
func (s *Store) UpdateTaskProgress(id uint, status string, progress int) error {
	var task ScanTask
	if err := s.db.First(&task, id).Error; err != nil {
		return err
	}
	// Don't downgrade status if it's already finished
	if task.Status == "completed" || task.Status == "failed" || task.Status == "cancelled" {
		return nil
	}

	updates := map[string]interface{}{
		"progress": progress,
		"status":   status,
	}
	if status == "completed" || status == "failed" {
		now := time.Now()
		updates["completed_at"] = &now
	}
	return s.db.Model(&task).Updates(updates).Error
}
