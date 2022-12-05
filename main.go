package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/epsniff/expodb/pkg/server"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

func main() {

	var cfg = zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	args := config.ParseArgs()

	ecfg, err := config.LoadConfig(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration errors - %s\n", err)
		os.Exit(1)
	}
	logger = logger.Named(ecfg.ID())

	serf.DefaultConfig()

	srv, err := server.New(ecfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring node: %s", err)
		os.Exit(1)
	}

	// Handler for Ctrl+C and then shutdown the server.
	ctx, can := context.WithCancel(context.Background())
	defer can()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() error {
		<-signalChan
		can()
		return nil
	}()

	if err := srv.Serve(ctx); err != nil {
		os.Exit(-1)
	}
}
