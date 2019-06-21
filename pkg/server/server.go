package server

import (
	"context"
	"net"
	"os"
	"os/signal"

	"github.com/epsniff/expodb/pkg/config"
	raftagent "github.com/epsniff/expodb/pkg/server/agents/raft"
	serfagent "github.com/epsniff/expodb/pkg/server/agents/serf"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type server struct {
	config    *config.Config
	raftNode  *raft.Raft
	fsm       *fsm
	logger    *zap.Logger
	serfAgent *serfagent.Agent
	raftAgent *raftagent.Agent
}

func (n *server) IsLeader() bool {
	return n.raftNode.State() == raft.Leader
}

func (n *server) Serve() {
	ctx, can := context.WithCancel(context.Background())
	defer can()
	g, ctx := errgroup.WithContext(ctx)

	// Run HTTP server
	g.Go(func() error {
		// Construct HTTP Address
		httpAddr := &net.TCPAddr{
			IP:   net.ParseIP(n.config.HTTPBindAddress),
			Port: n.config.HTTPBindPort,
		}
		httpServer := &httpServer{
			node:    n,
			address: httpAddr,
			logger:  n.logger.Named("http"),
		}
		go httpServer.Start() // there isn't a wait to use a context to cancel an http server?? so just spin it off in a go routine for now.
		return nil
	})

	// Run serf agent
	g.Go(func() error {
		if err := n.serfag.Start(); err != nil {
			can()
			n.logger.Error("serf agent failed to start", zap.Error(err))
			return err
		}
		n.logger.Info("serf agent started", zap.Bool("isSeed", n.config.IsSerfSeed), zap.String("node-name", n.serfag.SerfConfig().NodeName))
		if !n.config.IsSerfSeed {
			n.logger.Info("joining serf cluster using", zap.Strings("peers", n.config.SerfJoinAddrs))
			const replay = false
			n.serfag.Join(n.config.SerfJoinAddrs, replay)
		}

		<-n.serfag.ShutdownCh() // wait for the serf agent to shutdown
		can()
		n.logger.Info("The serf agent shutdown successfully")
		return nil
	})

	// Handler for Ctrl+C and then call n.serfag.Shutdown()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	g.Go(func() error {
		select {
		case <-ctx.Done():
		case <-signalChan:
			can()
		}
		return nil
	})

	// Go routine to cleanup seft agent on shutdown
	g.Go(func() error {
		<-ctx.Done()
		n.logger.Info("Stopping serf agent")
		return n.serfag.Shutdown()
	})

	// Go routine to cleanup raft agent on shutdown
	g.Go(func() error {
		<-ctx.Done()
		n.logger.Info("Stopping raft agent")
		raftFuture := n.raftNode.Shutdown()
		return raftFuture.Error()
	})

	n.logger.Info("Server started")
	if err := g.Wait(); err != nil {
		n.logger.Warn("Child workers returned an error")
	} else {
		n.logger.Info("Clean shutdown")
	}
}
