package multiraft

import (
	"context"
	"fmt"
	"time"

	machines "github.com/epsniff/expodb/pkg/server/machines"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dgConfig "github.com/lni/dragonboat/v4/config"
)

// Agent starts and manages a raft server and the primary FSM.
type Agent struct {
	ctx       context.Context
	replicaID uint64
	shardID   uint64
	nh        *dragonboat.NodeHost
	cs        *client.Session
}

func New(nh *dragonboat.NodeHost, replicaID, shardID uint64, initialMembers map[uint64]string, datadir string) (*Agent, error) {
	a := &Agent{
		ctx:       context.Background(),
		nh:        nh,
		replicaID: replicaID,
		shardID:   shardID,
	}
	// config for raft
	rc := dgConfig.Config{
		ReplicaID:          replicaID,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
		ShardID:            shardID,
	}

	if err := nh.StartOnDiskReplica(initialMembers, len(initialMembers) == 0, NewDiskKV(datadir), rc); err != nil {
		return nil, fmt.Errorf("starting replica: %w", err)
	}
	a.nh = nh
	a.replicaID = replicaID
	return a, nil
}

// AddVoter adds a voting peer to the raft consenses group.
// Can only be called on the leader.
func (a *Agent) AddVoter(replicaID uint64, peerAddress string) error {
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()
	return a.nh.SyncRequestAddReplica(ctx, a.shardID, replicaID, peerAddress, 0)
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner.  This call blocks until the log is conserted commited or
// until 5 seconds is reached.
func (a *Agent) Apply(val machines.RaftEntry) error {
	// TODO: reuse the session?
	a.cs = a.nh.GetNoOPSession(a.shardID)
	data, err := val.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal raft entry: %w", err)
	}
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()
	_, err = a.nh.SyncPropose(ctx, a.cs, data)
	return err
}

func (a *Agent) Read(query interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()
	res, err := a.nh.SyncRead(ctx, a.shardID, query)
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}
	return res, err
}

// Shutdown stops the raft server.
func (a *Agent) Shutdown() error {
	a.nh.Close()
	return nil
}
