package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/bane-labs/bridge-indexer-backend/internal/server"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := server.New()
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	fmt.Println("Bridge Indexer Backend starting on :8080")

	// Start server with timeout
	server := &http.Server{
		Addr:    ":8080",
		Handler: srv,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server failed to start: %v", err)
			cancel()
		}
	}()

	// Wait for SIGTERM signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-ctx.Done():
		fmt.Println("Server context cancelled")
	case sig := <-sigChan:
		fmt.Printf("Received signal: %v\n", sig)
		cancel()
	}

	fmt.Println("Shutting down server...")
	os.Exit(0)
}
