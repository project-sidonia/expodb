package server

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

func (n *server) monitorLeadership(ctx context.Context) error {

	leaderCh := n.raftAgent.LeaderNotifyCh()
	var lCtx context.Context
	var lCan context.CancelFunc
	var leaderLoop sync.WaitGroup
	for {
		select {
		case isLeader := <-leaderCh:
			switch {
			case isLeader:
				if lCtx != nil {
					n.logger.Warn("attempted to start the leader loop while running", zap.String("id", n.config.ID()))
					continue
				}
				lCtx, lCan = context.WithCancel(context.Background())
				leaderLoop.Add(1)
				go func(ctx context.Context) {
					defer leaderLoop.Done()
					n.leaderLoop(ctx)
				}(lCtx)
				n.logger.Info("cluster leadership acquired", zap.String("id", n.config.ID()))

			default:
				if lCtx == nil {
					n.logger.Warn("attempted to stop the leader loop while not running", zap.String("id", n.config.ID()))
					continue
				}
				n.logger.Info("shutting down leader loop", zap.String("id", n.config.ID()))
				lCan()
				leaderLoop.Wait()
				lCtx = nil
				n.logger.Info("cluster leadership lost", zap.String("id", n.config.ID()))
			}
		case <-ctx.Done():
			if lCtx != nil {
				lCan()
			}
			return nil
		}
	}
}

func (n *server) leaderLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			n.logger.Info("leader loop exiting")
			return nil
		}
		// We are the leader, do leader stuff here.
	}
	return nil
}
