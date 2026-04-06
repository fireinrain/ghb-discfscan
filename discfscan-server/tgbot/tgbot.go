package tgbot

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/gommon/log"

	tele "gopkg.in/telebot.v3"

	"discfscan-server/pkg"
)

type TGBotManager struct {
	mu         sync.Mutex
	bot        *tele.Bot
	store      *pkg.Store
	dispatcher *pkg.GitHubDispatcher
}

func NewTGBotManager(store *pkg.Store, dispatcher *pkg.GitHubDispatcher) *TGBotManager {
	return &TGBotManager{
		store:      store,
		dispatcher: dispatcher,
	}
}

func (m *TGBotManager) Start(token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop existing bot if running
	if m.bot != nil {
		m.bot.Stop()
	}

	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("empty token provided")
	}

	pref := tele.Settings{
		Token:  token,
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	}

	b, err := tele.NewBot(pref)
	if err != nil {
		log.Errorf("[TGBot] Initialization failed using token [%s...]: %v", token[:4], err)
		return err
	}

	m.bot = b
	m.registerRoutes()

	go func() {
		log.Info("Telegram bot is starting...")
		b.Start()
	}()

	return nil
}

func (m *TGBotManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.bot != nil {
		m.bot.Stop()
		m.bot = nil
	}
	log.Info("Telegram bot stopped.")
}

func (m *TGBotManager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bot != nil
}

func (m *TGBotManager) SendTestMessage(chatID int64, text string) error {
	m.mu.Lock()
	b := m.bot
	m.mu.Unlock()

	if b == nil {
		return fmt.Errorf("bot is not running")
	}

	chat := &tele.Chat{ID: chatID}
	_, err := b.Send(chat, text)
	return err
}

func (m *TGBotManager) registerRoutes() {
	// Global Middleware: Apply Access Control Layer
	m.bot.Use(func(next tele.HandlerFunc) tele.HandlerFunc {
		return func(c tele.Context) error {
			sender := c.Sender()
			chat := c.Chat()
			text := c.Text()

			cmd := text
			if len(text) > 0 && text[0] == '/' {
				for i, ch := range text {
					if ch == ' ' || ch == '@' {
						cmd = text[:i]
						break
					}
				}
			} else {
				return nil // Drop any non-command messages
			}

			// Validate if the specific Sender/User natively has this permission in this Chat
			if !m.store.VerifyTGCommand(sender.ID, chat.ID, cmd) {
				log.Infof("[TGBot] Blocked unauthorized execution attempt: UserID=%d, ChatID=%d, Command=%s", sender.ID, chat.ID, cmd)
				return nil // Drop silently to prevent spam
			}

			return next(c)
		}
	})

	m.bot.Handle("/scan", func(c tele.Context) error {
		args := c.Args()
		if len(args) < 2 {
			return c.Reply("用法: /run <task_type> <target> [port] [speedtest] [workers]")
		}

		taskType := args[0]
		target := args[1]
		port := "443,2053,2083,2087,2096,8443" // Default
		if len(args) > 2 {
			port = args[2]
		}
		speedtest := "false"
		if len(args) > 3 {
			speedtest = args[3]
		}
		workers := ""
		if len(args) > 4 {
			workers = args[4]
		}

		label := "Telegram Task"
		desc := "Triggered by Telegram User"

		// Pre-calculate IP and Worker Counts
		targets, _ := pkg.ParseCIDRList(target)
		ipCount := pkg.CountTotalIPs(targets)

		defaultMaxStr := m.store.GetConfig("github_workers")
		defaultMax := 10
		if n, err := strconv.Atoi(defaultMaxStr); err == nil && n > 0 {
			defaultMax = n
		}
		workerCount := pkg.DecideWorkerCount(ipCount, defaultMax)
		if workers != "" {
			if n, err := strconv.Atoi(workers); err == nil && n > 0 {
				workerCount = n
			}
		}

		task, err := m.store.CreateTask(label, desc, taskType, target, port, speedtest, workers, ipCount, workerCount)
		if err != nil {
			log.Errorf("[TGBot] Task creation failed: %v", err)
			return c.Reply("❌ 创建扫描任务失败: " + err.Error())
		}

		log.Infof("[TGBot] Task %d created via Telegram by %s", task.ID, c.Sender().Username)
		return c.Reply(fmt.Sprintf("✅ 扫描任务已加入队列!\nID: %d\n目标: %s\n状态: 待处理 (Pending)", task.ID, target))
	})

	m.bot.Handle("/ping", func(c tele.Context) error {
		return c.Reply("pong!")
	})
}
