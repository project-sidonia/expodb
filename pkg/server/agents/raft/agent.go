package raft

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/epsniff/expodb/pkg/loggingutils"
	"github.com/epsniff/expodb/pkg/server/agents/raft/machines"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

// RaftEntry all log entry most support this interface.
type RaftEntry interface {
	// By convention the messages self marshal and encode thier fsm type as the last
	// 2 bytes of the bytes.
	Marshal() ([]byte, error)
}

// Agent starts and manages a raft server and the primary FSM.
type Agent struct {
	logger *zap.Logger

	raftNode *raft.Raft
	fsm      *fsm
}

func New(config *config.Config, logger *zap.Logger) (*Agent, error) {
	fsmp := machines.FSMProvider{}
	fsm := &fsm{
		fsmProvider: fsmp,
		logger:      logger,
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
		logger:   logger,
		raftNode: raftNode,
		fsm:      fsm}, nil
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
func (a *Agent) Apply(key uint16, val RaftEntry) error {
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

// AddStateMachine adds a child state machine that gets a subset of the logs.
// The key is encoded into the raft messages and each child FSM is required to
// Marshal that key into the last 2 bytes of it's message types.  This is used
// for routing messages by the primary FSM.
func (a *Agent) AddStateMachine(key uint16, sm machines.StateMachine) error {
	return a.fsm.fsmProvider.Add(key, sm)
}

func (a *Agent) IsLeader() bool {
	return a.raftNode.State() == raft.Leader
}

func (a *Agent) LeaderAddress() string {
	leaderAddr := a.raftNode.Leader()
	return string(leaderAddr)
}

func (a *Agent) Shutdown() error {
	raftFuture := a.raftNode.Shutdown()
	return raftFuture.Error()
}
