package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"discfscan-server/api"
	"discfscan-server/pkg"
	"discfscan-server/tgbot"
)

func main() {
	// Initialize Dependencies
	store, err := pkg.NewStore()
	if err != nil {
		log.Fatalf("Critical Error: failed to initialize sqlite db: %v", err)
	}

	dispatcher := pkg.NewGitHubDispatcher(store)
	tgBotManager := tgbot.NewTGBotManager(store, dispatcher)

	if token := store.GetConfig("tg_token"); token != "" {
		tgBotManager.Start(token)
	}

	// 1. Initialize API Server
	apiServer := api.NewAPIServer(store, dispatcher, tgBotManager)
	apiServer.StartDispatcher()
	go func() {
		err := apiServer.Start(":9999")
		if err != nil {
			log.Println("Echo server shutdown:", err)
		}
	}()
	log.Println("API Server started on http://127.0.0.1:9999")

	// Wait for OS signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down servers...")
}
