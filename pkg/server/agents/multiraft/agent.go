package multiraft

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/epsniff/expodb/pkg/config"
	machines "github.com/epsniff/expodb/pkg/server/state-machines"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dgConfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
)

// RaftEntry all log entry most support this interface.
type RaftEntry interface {
	// By convention the messages self marshal and encode thier fsm type as the last
	// 2 bytes of the bytes.
	Marshal() ([]byte, error)
}

// Agent starts and manages a raft server and the primary FSM.
type Agent struct {
	ctx       context.Context
	replicaID uint64
	nh        *dragonboat.NodeHost
	cs        *client.Session

	raftNode     *raft.Raft
	raftNotifyCh chan bool
	fsm          *fsm
}

const (
	// we use two raft groups in this example, they are identified by the cluster
	// ID values below
	shardID1 uint64 = 100
)

var (
	// initial nodes count is three, their addresses are also fixed
	// this is for simplicity
	addresses = []string{
		"localhost:63001",
		"localhost:63002",
		"localhost:63003",
	}
)

func New(config *config.Config, _ *zap.Logger) (*Agent, error) {
	replicaID, err := strconv.ParseUint(config.ID(), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse nodeid %s: %w", config.NodeName, err)
	}
	name := fmt.Sprintf("%s:%d", config.RaftBindAddress, config.RaftBindPort)
	nodeAddr := base64.StdEncoding.EncodeToString([]byte(name))
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	// change the log verbosity
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	// config for raft
	// note the ShardID value is not specified here
	rc := dgConfig.Config{
		ReplicaID:          uint64(replicaID),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	datadir := filepath.Join(
		"example-data",
		"multigroup-data",
		fmt.Sprintf("node%d", replicaID))
	// config for the nodehost
	// by default, insecure transport is used, you can choose to use Mutual TLS
	// Authentication to authenticate both servers and clients. To use Mutual
	// TLS Authentication, set the MutualTLS field in NodeHostConfig to true, set
	// the CAFile, CertFile and KeyFile fields to point to the path of your CA
	// file, certificate and key files.
	// by default, TCP based RPC module is used, set the RaftRPCFactory field in
	// NodeHostConfig to rpc.NewRaftGRPC (github.com/lni/dragonboat/plugin/rpc) to
	// use gRPC based transport. To use gRPC based RPC module, you need to install
	// the gRPC library first -
	//
	// $ go get -u google.golang.org/grpc
	//
	a := &Agent{
		ctx:          context.Background(),
		raftNotifyCh: make(chan bool, 1),
	}
	nhc := dgConfig.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr,
		// RaftRPCFactory: rpc.NewRaftGRPC,
		RaftEventListener: a,
	}
	// create a NodeHost instance. it is a facade interface allowing access to
	// all functionalities provided by dragonboat.
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, fmt.Errorf("failed to create nodehost, %v", err)
	}
	// start the first cluster
	// we use ExampleStateMachine as the IStateMachine for this cluster, its
	// behaviour is identical to the one used in the Hello World example.
	rc.ShardID = shardID1

	var members map[uint64]string
	if config.Bootstrap {
		initialMembers := make(map[uint64]string)
		for idx, v := range addresses {
			if uint64(idx+1) == replicaID {
				initialMembers[uint64(idx+1)] = nodeAddr
				continue
			}
			// key is the ReplicaID, ReplicaID is not allowed to be 0
			// value is the raft address
			initialMembers[uint64(idx+1)] = v
		}
		members = initialMembers
	} else {
		mem, err := nh.SyncGetShardMembership(a.ctx, shardID1)
		if err != nil {
			return nil, fmt.Errorf("failed to get shard membership, %w", err)
		}
		members = mem.Nodes
	}
	if err := nh.StartReplica(members, false, NewFSM, rc); err != nil {
		return nil, fmt.Errorf("failed to add cluster, %w", err)
	}
	cs, err := nh.SyncGetSession(a.ctx, shardID1)
	if err != nil {
		return nil, fmt.Errorf("failed to get session, %w", err)
	}
	a.cs = cs
	return a, nil
}

// NotifyCh is a channel that returns true if this agent has been elected as the
// raft leader.
func (a *Agent) LeaderNotifyCh() <-chan bool {
	return a.raftNotifyCh
}

func (a *Agent) LeaderUpdated(info raftio.LeaderInfo) {
	if info.ShardID == shardID1 {
		a.raftNotifyCh <- info.LeaderID == a.replicaID
	}
}

// AddVoter adds a voting peer to the raft consenses group.
// Can only be called on the leader.
func (a *Agent) AddVoter(id, peerAddress string) error {
	ui64, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse id: %w", err)
	}
	return a.nh.SyncRequestAddReplica(a.ctx, shardID1, ui64, peerAddress, 0)
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner.  This call blocks until the log is conserted commited or
// until 5 seconds is reached.
func (a *Agent) Apply(key uint16, val RaftEntry) error {
	data, err := val.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal raft entry: %w", err)
	}
	_, err = a.nh.SyncPropose(a.ctx, a.cs, data)
	return err
}

// AddStateMachine adds a child state machine that gets a subset of the logs.
// The key is encoded into the raft messages and each child FSM is required to
// Marshal that key into the last 2 bytes of it's message types.  This is used
// for routing messages by the primary FSM.
func (a *Agent) AddStateMachine(key uint16, sm machines.StateMachine) error {
	return a.fsm.fsmProvider.Add(key, sm)
}

// IsLeader returns true if this agent is the leader.
func (a *Agent) IsLeader() bool {
	leaderID, _, ok, err := a.nh.GetLeaderID(shardID1)
	return ok && err == nil && a.replicaID == leaderID
}

// LeaderAddr returns the address of the leader.
func (a *Agent) LeaderAddress() string {
	leaderAddr := a.raftNode.Leader()
	return string(leaderAddr)
}

// Shutdown stops the raft server.
func (a *Agent) Shutdown() error {
	a.nh.Close()
	return nil
}
