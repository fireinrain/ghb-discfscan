package tgbot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var ctx = context.Background()

var rdb = redis.NewClient(&redis.Options{
	Addr: os.Getenv("REDIS_ADDR"),
})

func RunTgBotServer() {
	bot, _ := tgbotapi.NewBotAPI(os.Getenv("TG_TOKEN"))

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil {
			continue
		}

		text := update.Message.Text

		if strings.HasPrefix(text, "/run") {
			handleRun(bot, update.Message)
		}
	}
}

func handleRun(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	args := strings.Split(msg.Text, " ")

	if len(args) < 3 {
		reply(bot, msg.Chat.ID, "用法: /run scan target")
		return
	}

	taskType := args[1]
	target := args[2]

	taskID := fmt.Sprintf("%d", msg.MessageID)

	task := map[string]string{
		"id":     taskID,
		"type":   taskType,
		"target": target,
	}

	data, _ := json.Marshal(task)

	// 写入队列
	rdb.LPush(ctx, "queue:task", data)

	// 初始化状态
	rdb.HSet(ctx, "task:"+taskID, "status", "pending")

	// 触发 GitHub Actions
	triggerWorkflow()

	reply(bot, msg.Chat.ID, "任务已提交："+taskID)
}

func triggerWorkflow() {
	url := "https://api.github.com/repos/你的用户名/你的仓库/actions/workflows/worker.yml/dispatches"

	payload := []byte(`{
		"ref": "main"
	}`)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	req.Header.Set("Authorization", "Bearer "+os.Getenv("GITHUB_TOKEN"))
	req.Header.Set("Accept", "application/vnd.github+json")

	http.DefaultClient.Do(req)
}

func reply(bot *tgbotapi.BotAPI, chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	bot.Send(msg)
}
