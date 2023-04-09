package raft

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/epsniff/expodb/pkg/loggingutils"
	machines "github.com/epsniff/expodb/pkg/server/state-machines"
	"github.com/epsniff/expodb/pkg/server/state-machines/simplestore"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

// Agent starts and manages a raft server and the primary FSM.
type Agent struct {
	logger *zap.Logger

	raftNode     *raft.Raft
	raftNotifyCh chan bool
	fsm          *fsm
}

// New creates a new raft server.
func New(config *config.Config, logger *zap.Logger) (*Agent, error) {
	fsm := &fsm{
		st:     simplestore.New(),
		logger: logger,
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
		return nil, err
	}

	raftNotifych := make(chan bool, 1)
	raftConfig.NotifyCh = raftNotifych

	snapshotStoreLogger := logger.Named("raft.snapshots")
	const retain = 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		config.RaftDataDir,
		retain,
		loggingutils.NewLogWriter(snapshotStoreLogger),
	)
	if err != nil {
		return nil, err
	}
	logStore, err := raftboltdb.NewBoltStore(
		filepath.Join(config.RaftDataDir, "raft-log.bolt"),
	)
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(config.RaftDataDir, "raft-stable.bolt"),
	)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	if config.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
		if err != nil {
			return nil, err
		}
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raftConfig.LocalID,
						Address: transport.LocalAddr(),
					},
				},
			}
			logger.Info("bootstrapping node")

			f := raftNode.BootstrapCluster(configuration)
			err := f.Error()
			if err != nil {
				return nil, err
			}
		}
	}

	return &Agent{
		logger:       logger,
		raftNode:     raftNode,
		raftNotifyCh: raftNotifych,
		fsm:          fsm}, nil
}

// NotifyCh is a channel that returns true if this agent has been elected as the
// raft leader.
func (a *Agent) LeaderNotifyCh() <-chan bool {
	return a.raftNotifyCh
}

func (a *Agent) GetByRowKey(table, rowKey string) (map[string]string, error) {
	query := simplestore.Query{
		Table:  table,
		RowKey: rowKey,
	}
	res, err := a.fsm.st.Lookup(query)
	return res.(map[string]string), err
}

// AddVoter adds a voting peer to the raft consenses group.
// Can only be called on the leader.
func (a *Agent) AddVoter(id, peerAddress string) error {
	f := a.raftNode.AddVoter(raft.ServerID(id), raft.ServerAddress(peerAddress), 0, 10*time.Second)
	return f.Error()
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner.  This call blocks until the log is conserted commited or
// until 5 seconds is reached.
func (a *Agent) Apply(val machines.RaftEntry) error {
	data, err := val.Marshal()
	if err != nil {
		a.logger.Error("Failed to marshal raft entry", zap.Error(err))
		return err
	}
	applyFuture := a.raftNode.Apply(data, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		a.logger.Error("Failed to apply raft log", zap.Error(err))
		return err
	}
	return nil
}

// IsLeader returns true if this agent is the leader.
func (a *Agent) IsLeader() bool {
	return a.raftNode.State() == raft.Leader
}

// LeaderAddr returns the address of the leader.
func (a *Agent) LeaderAddress() string {
	leaderAddr := a.raftNode.Leader()
	return string(leaderAddr)
}

// Shutdown stops the raft server.
func (a *Agent) Shutdown() error {
	raftFuture := a.raftNode.Shutdown()
	return raftFuture.Error()
}
