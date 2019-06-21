package raft

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"time"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/epsniff/expodb/pkg/loggingutils"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

func New(config *config.Config, logger *zap.Logger) (*raft.Raft, *fsm, error) {
	fsm := &fsm{
		stateValue: map[string]int{},
		logger:     logger,
	}

	// Construct Raft Address
	raftAddr := &net.TCPAddr{
		IP:   net.ParseIP(config.RaftBindAddress),
		Port: config.RaftBindPort,
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(raftAddr.String())
	raftConfig.Logger = loggingutils.NewHclog2ZapLogger(logger)
	transportLogger := logger.Named("raft.transport")
	transport, err := raftTransport(
		raftAddr,
		loggingutils.NewLogWriter(transportLogger),
	)
	if err != nil {
		return nil, nil, err
	}

	snapshotStoreLogger := logger.Named("raft.snapshots")
	const retain = 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		config.RaftDataDir,
		retain,
		loggingutils.NewLogWriter(snapshotStoreLogger),
	)
	if err != nil {
		return nil, nil, err
	}
	logStore, err := raftboltdb.NewBoltStore(
		filepath.Join(config.RaftDataDir, "raft-log.bolt"),
	)
	if err != nil {
		return nil, nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(config.RaftDataDir, "raft-stable.bolt"),
	)
	if err != nil {
		return nil, nil, err
	}

	logger.Info("NewNode created raft node", zap.String("raft-config", fmt.Sprintf("%+v", raftConfig)))

	raftNode, err := raft.NewRaft(
		raftConfig,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return nil, nil, err
	}

	if config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}

	return raftNode, fsm, nil
}

func raftTransport(raftAddr net.Addr, log io.Writer) (*raft.NetworkTransport, error) {
	address, err := net.ResolveTCPAddr("tcp", raftAddr.String())
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(address.String(), address, 3, 10*time.Second, log)
	if err != nil {
		return nil, err
	}

	return transport, nil
}
