package main

import (
	"fmt"
	"log"
	"os"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/epsniff/expodb/pkg/server"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	defer logger.Sync()

	config, err := config.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration errors - %s\n", err)
		os.Exit(1)
	}
	logger = logger.Named(config.ID())

	serf.DefaultConfig()

	srv, err := server.New(config, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring node: %s", err)
		os.Exit(1)
	}
	if err := srv.Serve(); err != nil {
		os.Exit(-1)
	}
}
