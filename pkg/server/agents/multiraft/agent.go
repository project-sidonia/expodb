package multiraft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/epsniff/expodb/pkg/config"
	machines "github.com/epsniff/expodb/pkg/server/state-machines"
	"github.com/epsniff/expodb/pkg/server/state-machines/simplestore"
	"go.uber.org/zap"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dgConfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
)

// Agent starts and manages a raft server and the primary FSM.
type Agent struct {
	ctx       context.Context
	replicaID uint64
	nh        *dragonboat.NodeHost
	cs        *client.Session

	raftNotifyCh chan bool
}

const (
	// we use two raft groups in this example, they are identified by the cluster
	// ID values below
	shardID1 uint64 = 100
)

func New(config *config.Config, _ *zap.Logger) (*Agent, error) {
	replicaID, err := strconv.ParseUint(config.ID(), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse nodeid %s: %w", config.NodeName, err)
	}
	nodeAddr := fmt.Sprintf("%s:%d", config.RaftBindAddress, config.RaftBindPort)
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
		WALDir:            datadir,
		NodeHostDir:       datadir,
		RTTMillisecond:    200,
		RaftAddress:       nodeAddr,
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

	members := map[uint64]string{}
	if config.Bootstrap {
		members[replicaID] = nodeAddr
	}
	if err := nh.StartReplica(members, len(members) == 0, NewFSM, rc); err != nil {
		return nil, fmt.Errorf("failed to add cluster, %w", err)
	}
	//if err := nh.StartOnDiskReplica(members, len(members) == 0, NewDiskKV, rc); err != nil {
	//	return nil, fmt.Errorf("failed to add cluster, %w", err)
	//}
	a.cs = nh.GetNoOPSession(shardID1)
	a.nh = nh
	a.replicaID = replicaID
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
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()
	return a.nh.SyncRequestAddReplica(ctx, shardID1, ui64, peerAddress, 0)
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner.  This call blocks until the log is conserted commited or
// until 5 seconds is reached.
func (a *Agent) Apply(val machines.RaftEntry) error {
	data, err := val.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal raft entry: %w", err)
	}
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()
	_, err = a.nh.SyncPropose(ctx, a.cs, data)
	return err
}

func (a *Agent) GetByRowKey(table, rowKey string) (map[string]string, error) {
	query := simplestore.Query{
		Table:  table,
		RowKey: rowKey,
	}
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()
	res, err := a.nh.SyncRead(ctx, shardID1, query)
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}
	resp, ok := res.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("failed to convert result to map[string]string: %T", res)
	}
	return resp, err
}

// IsLeader returns true if this agent is the leader.
func (a *Agent) IsLeader() bool {
	leaderID, _, ok, err := a.nh.GetLeaderID(shardID1)
	return ok && err == nil && a.replicaID == leaderID
}

// Shutdown stops the raft server.
func (a *Agent) Shutdown() error {
	a.nh.Close()
	return nil
}
