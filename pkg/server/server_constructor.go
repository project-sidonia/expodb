package server

import (
	"os"

	"github.com/epsniff/expodb/pkg/config"
	serfagent "github.com/epsniff/expodb/pkg/server/agents/serf"
	"go.uber.org/zap"
)

func New(config *config.Config, logger *zap.Logger) (*server, error) {

	if err := os.MkdirAll(config.SerfDataDir, 0700); err != nil {
		return nil, err
	}
	serfag, err := serfagent.New(config, logger.Named("serf-agent"))
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(config.RaftDataDir, 0700); err != nil {
		return nil, err
	}
	raftNode, fsm, err := setupRaftServer(config, logger)
	if err != nil {
		return nil, err
	}

	return &server{
		config:   config,
		raftNode: raftNode,
		logger:   logger,
		fsm:      fsm,
		serfag:   serfag,
	}, nil
}
