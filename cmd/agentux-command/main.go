package main

import (
	"log"
	"net/http"
	"os"

	"github.com/agentux-os/agentux-command/internal/server"
)

func main() {
	address := os.Getenv("AGENTUX_COMMAND_ADDR")
	if address == "" {
		address = ":8080"
	}
	log.Printf("agentux-command listening on %s", address)
	if err := http.ListenAndServe(address, server.NewHandler()); err != nil {
		log.Fatal(err)
	}
}
