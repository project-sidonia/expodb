package server

import (
	"os"

	"github.com/epsniff/expodb/pkg/config"
	"go.uber.org/zap"
)

func New(config *config.Config, logger *zap.Logger) (*server, error) {

	if err := os.MkdirAll(config.SerfDataDir, 0700); err != nil {
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
	}, nil
}
