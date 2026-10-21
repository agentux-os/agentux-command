package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agentux-os/agentux-command/internal/server"
)

func run() error {
	address := os.Getenv("AGENTUX_COMMAND_ADDR")
	if address == "" {
		address = ":8080"
	}
	startupContext, cancelStartup := context.WithTimeout(context.Background(), 30*time.Second)
	handler, closeStore, err := server.NewPostgresHandler(startupContext, os.Getenv("AGENTUX_COMMAND_DATABASE_URL"))
	cancelStartup()
	if err != nil {
		return err
	}
	defer closeStore()
	// Each node authenticates with its own token: node-1=token,node-2=token.
	if tokens := server.ParseNodeTokens(os.Getenv("AGENTUX_COMMAND_NODE_TOKENS")); len(tokens) > 0 {
		handler.WithNodeTokens(tokens)
	} else {
		log.Printf("AGENTUX_COMMAND_NODE_TOKENS is not set: protocol messages are not authenticated")
	}

	serviceContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	httpServer := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- httpServer.ListenAndServe() }()
	log.Printf("agentux-command listening on %s", address)
	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-serviceContext.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownContext)
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
