package server

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v4"
	dgConfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/project-sidonia/expodb/pkg/config"
	"github.com/project-sidonia/expodb/pkg/server/agents/multiraft"
	serfagent "github.com/project-sidonia/expodb/pkg/server/agents/serf"
	"github.com/project-sidonia/expodb/pkg/server/machines"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	numShards int = 5
)

type server struct {
	config *config.Config
	logger *zap.Logger

	metadata *metadata

	serfAgent *serfagent.Agent

	raftNotifyCh chan struct {
		bool
		uint64
	}
	nh *dragonboat.NodeHost

	// Current shards started on this node.
	raftAgentsMu sync.Mutex
	raftAgents   map[uint64]bool

	consistent *consistent.Consistent

	replicaID uint64
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner.  This call blocks until the log is conserted commited or
// until 5 seconds is reached.
func (a *server) Apply(ctx context.Context, shardID uint64, val machines.RaftEntry) error {
	// TODO: reuse the session?
	cs := a.nh.GetNoOPSession(shardID)
	data, err := val.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal raft entry: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Equivalent to:
	//nu, _ := a.nh.GetNodeUser(shardID)
	//rq, _ := nu.Propose(cs, data, 5 * time.Second)
	_, err = a.nh.SyncPropose(ctx, cs, data)
	return err
}

func (a *server) Read(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Equivalent to:
	//nu, _ := a.nh.GetNodeUser(shardID)
	//rq, _ := nu.ReadIndex(5 * time.Second)
	//a.nh.ReadLocalNode(rq, query)
	res, err := a.nh.SyncRead(ctx, shardID, query)
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}
	return res, err
}

// Shutdown stops the raft server.
func (a *server) Shutdown() error {
	a.nh.Close()
	return nil
}

// IsLeader returns true if this agent is the leader.
func (n *server) isLeader(shardID uint64) (bool, error) {
	leaderID, _, ok, err := n.nh.GetLeaderID(shardID)
	return ok && n.replicaID == leaderID, err
}

func (n *server) pickShard(table, rowKey string) uint64 {
	f := fnv.New64a()
	f.Write([]byte(table))
	f.Write([]byte(rowKey))
	return (f.Sum64() % uint64(numShards)) + 1
}

// GetByRowKey gets a value from the raft key value fsm
func (n *server) GetByRowKey(table, rowKey string) (map[string]string, error) {
	query := machines.Query{
		Table:  table,
		RowKey: rowKey,
	}
	// TODO: forward request if we don't have the given shard
	shardID := n.pickShard(table, rowKey)
	val, err := n.Read(context.Background(), shardID, query)
	if err != nil {
		return nil, err
	}
	resp, ok := val.(map[string]string)
	if !ok {
		return nil, fmt.Errorf("converting result to map[string]string: %T", val)
	}
	return resp, err
}

// SetKeyVal sets a value in the raft key value fsm, if we aren't the
// current leader then forward the request onto the leader node.
func (n *server) SetKeyVal(table, rowKey, col, val string) error {
	//kve := simplestore.NewKeyValEvent(simplestore.UpdateRowOp, table, col, key, val)
	kve := multiraft.KVData{Key: table + ":" + rowKey + ":" + col, Val: val}
	shardID := n.pickShard(table, rowKey)
	err := n.Apply(context.Background(), shardID, kve)
	return err
}

func parseNodeID(nodeName string) (uint64, error) {
	// Assumes "node-1", "node-2", etc.
	parts := strings.Split(nodeName, "-")
	replicaID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse nodeid %s: %w", nodeName, err)
	}
	return replicaID, nil
}

func New(config *config.Config, logger *zap.Logger) (*server, error) {
	if err := os.MkdirAll(config.SerfDataDir, 0700); err != nil {
		return nil, fmt.Errorf("making serf data dir: %w", err)
	}
	serfAgent, err := serfagent.New(config, logger.Named("serf-agent"))
	if err != nil {
		return nil, fmt.Errorf("creating serf agent: %w", err)
	}

	replicaID, err := parseNodeID(config.ID())
	if err != nil {
		return nil, err
	}

	ser := &server{
		config: config,
		logger: logger,

		metadata: NewMetadata(),

		serfAgent: serfAgent,
		raftNotifyCh: make(chan struct {
			bool
			uint64
		}, 1),
		replicaID:  replicaID,
		raftAgents: map[uint64]bool{},
	}

	if err := os.MkdirAll(config.RaftDataDir, 0700); err != nil {
		return nil, fmt.Errorf("making raft data dir: %w", err)
	}

	datadir := filepath.Join(config.RaftDataDir, "multigroup-data", config.ID())

	// change the log verbosity
	//logger.GetLogger("raft").SetLevel(logger.ERROR)
	//logger.GetLogger("rsm").SetLevel(logger.WARNING)
	//logger.GetLogger("transport").SetLevel(logger.WARNING)
	//logger.GetLogger("grpc").SetLevel(logger.WARNING)
	nhc := dgConfig.NodeHostConfig{
		WALDir:            datadir,
		NodeHostDir:       datadir,
		RTTMillisecond:    200,
		RaftAddress:       fmt.Sprintf("%s:%d", config.RaftBindAddress, config.RaftBindPort),
		RaftEventListener: ser,
	}
	// create a NodeHost instance. it is a facade interface allowing access to
	// all functionalities provided by dragonboat.
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, fmt.Errorf("failed to create nodehost, %w", err)
	}
	ser.nh = nh
	existingShards, err := ser.existingShards()
	if err != nil {
		return nil, fmt.Errorf("failed to get existing shards: %w", err)
	}
	for _, shardID := range existingShards {
		if err := ser.NewShard(false, false, uint64(shardID)); err != nil {
			return nil, fmt.Errorf("creating shard %d: %w", shardID, err)
		}
	}
	if len(existingShards) == 0 && config.Bootstrap {
		for shardID := 1; shardID < numShards+1; shardID++ {
			if err := ser.NewShard(config.Bootstrap, false, uint64(shardID)); err != nil {
				return nil, fmt.Errorf("creating shard %d: %w", shardID, err)
			}
		}
	}

	// register ourselfs as a handler for serf events. See (n *server) HandleEvent(e serf.Event)
	serfAgent.RegisterEventHandler(ser)

	return ser, nil
}

func (n *server) getStateMachineDir() string {
	return filepath.Join(n.config.RaftDataDir, "statemachine-data", n.config.ID())
}

//	func (n *server) shardDirExists(shardID uint64) (bool, error) {
//		datadir := filepath.Join(n.config.RaftDataDir, "statemachine-data", n.config.ID())
//		dir := multiraft.GetNodeDBDirName(datadir, shardID, n.replicaID)
//		if _, err := os.Stat(dir); err != nil && !os.IsNotExist(err) {
//			return false, fmt.Errorf("stat: %w", err)
//		} else if err == nil {
//			return true, nil
//		}
//		return false, nil
//	}
func (n *server) existingShards() ([]uint64, error) {
	datadir := filepath.Join(n.config.RaftDataDir, "statemachine-data", n.config.ID())
	items, err := os.ReadDir(datadir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading dir: %w", err)
	}
	shardIDs := []uint64{}
	var errors *multierror.Error
	for _, item := range items {
		if item.IsDir() {
			ss := strings.Split(item.Name(), "_")
			if len(ss) != 2 {
				continue
			}

			shardID, err := strconv.ParseUint(ss[0], 10, 64)
			if err != nil {
				errors = multierror.Append(errors, fmt.Errorf("parsing shard id %s: %w", ss[0], err))
			}
			shardIDs = append(shardIDs, shardID)
		}
	}
	return shardIDs, errors.ErrorOrNil()
}

func (n *server) NewShard(bootstrap bool, join bool, shardID uint64) error {
	n.raftAgentsMu.Lock()
	defer n.raftAgentsMu.Unlock()

	members := map[uint64]string{}
	if bootstrap && !join {
		members[n.replicaID] = n.nh.RaftAddress()
	}

	rc := dgConfig.Config{
		ReplicaID:          n.replicaID,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
		ShardID:            shardID,
	}

	if err := n.nh.StartOnDiskReplica(members, join, multiraft.NewDiskKV(n.getStateMachineDir()), rc); err != nil {
		return fmt.Errorf("starting replica: %w", err)
	}
	n.raftAgents[shardID] = true
	return nil
}

func (n *server) LeaderUpdated(info raftio.LeaderInfo) {
	// TODO (ajr) When we have actual multiraft!
	if v, ok := n.raftAgents[info.ShardID]; ok && v {
		n.raftNotifyCh <- struct {
			bool
			uint64
		}{info.LeaderID == n.replicaID, info.ShardID}
	}
}

// HandleEvent is our tap into serf events.  As the Serf(aka gossip) agent detects changes to the cluster
// state we'll handle the events in this handler.
func (n *server) HandleEvent(e serf.Event) {
	switch e.EventType() {
	case serf.EventMemberJoin:
		me := e.(serf.MemberEvent)
		n.logger.Info("Server Serf Handler: Member Join", zap.String("serf-event", fmt.Sprintf("%+v", me.Members)))

		for _, m := range me.Members {
			n.consistent.Add(myMember(m.Name))
			_, err := n.metadata.Add(m)
			if err != nil {
				n.logger.Error("Error processing metadata",
					zap.String("serf.Member", fmt.Sprintf("%+v", m)), zap.Error(err),
				)
			}
		}
	case serf.EventMemberReap, serf.EventMemberLeave:
		me := e.(serf.MemberEvent)
		n.logger.Info("Server Serf Handler: Member Leave/Reap", zap.String("serf-event", fmt.Sprintf("%+v", me)))
		for _, m := range me.Members {
			n.consistent.Remove(m.Name)
		}
	case serf.EventMemberFailed:
		me := e.(serf.MemberEvent)
		n.logger.Info("Server Serf Handler: Member Failed", zap.String("serf-event", fmt.Sprintf("%+v", me)))
	default:
		n.logger.Info("Server Serf Handler: Unhandled type", zap.String("serf-event", fmt.Sprintf("%+v", e)))
	}
}

func (n *server) scheduleShards(ctx context.Context) error {
	timer := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			desiredState := map[uint64][]consistent.Member{}
			currentState := map[uint64]map[uint64]string{}
			if len(n.consistent.GetMembers()) < 3 {
				n.logger.Warn("Not enough members to schedule shards", zap.Int("num_members", len(n.consistent.GetMembers())))
				timer.Reset(30 * time.Second)
				continue
			}
			for shardID := 1; shardID < numShards+1; shardID++ {
				members, err := n.consistent.GetClosestNForPartition(int(shardID)-1, 3)
				if err != nil {
					return fmt.Errorf("getting closest members: %w", err)
				}
				desiredState[uint64(shardID)] = members

				isLeader, err := n.isLeader(uint64(shardID))
				if err != nil {
					n.logger.Warn("Checking leadership", zap.Int("shard", shardID), zap.Error(err))
					//return fmt.Errorf("checking leader: %w", err)
				}
				for _, member := range members {
					if v, ok := n.raftAgents[uint64(shardID)]; n.config.ID() == member.String() && (!ok || !v) {
						// We are the closest member to this shard, so we should schedule it.
						if err := n.NewShard(false, true, uint64(shardID)); err != nil {
							return fmt.Errorf("creating shard %d: %w", shardID, err)
						}
					}
					if !isLeader {
						continue
					}
					// This does all the leader work.
					ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
					defer cancel()

					membership, err := n.nh.SyncGetShardMembership(ctx, uint64(shardID))
					if err != nil {
						return fmt.Errorf("getting shard membership: %w", err)
					}
					// shardID -> map of replicaID -> RaftAddress
					currentState[uint64(shardID)] = membership.Nodes

					nodedata, ok := n.metadata.FindByID(member.String())
					if !ok {
						n.logger.Error("Finding nodedata id", zap.String("member", member.String()))
						continue
					}
					// join new peer as a raft voter
					replicaID, err := parseNodeID(nodedata.ID())
					if err != nil {
						n.logger.Error("Error parsing nodedata id",
							zap.String("peer.id", nodedata.ID()),
							zap.String("peer.remoteaddr", nodedata.RaftAddr()),
							zap.Error(err),
						)
						continue
					}
					if _, ok := currentState[uint64(shardID)][replicaID]; ok {
						continue
					}
					err = n.nh.SyncRequestAddReplica(ctx, uint64(shardID), replicaID, nodedata.RaftAddr(), 0)
					if err != nil {
						n.logger.Error("Error joining peer to Raft",
							zap.String("peer.id", nodedata.ID()),
							zap.String("peer.remoteaddr", nodedata.RaftAddr()),
							zap.Error(err),
						)
					}
					n.logger.Info("Peer joined Raft", zap.String("peer.id", nodedata.ID()),
						zap.String("peer.remoteaddr", nodedata.RaftAddr()))
				}
			}
			timer.Reset(30 * time.Second)
		}
	}
}

// Serve runs the server's agents and blocks until one of the following:
// 1) An agent returns an error
// 2) A Ctrl-C signal is catch.
func (n *server) Serve() error {
	ctx, can := context.WithCancel(context.Background())
	defer can()
	g, ctx := errgroup.WithContext(ctx)

	// Run monitoring leadership
	g.Go(func() error {
		return n.monitorLeadership(ctx)
	})
	g.Go(func() error {
		return n.scheduleShards(ctx)
	})

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
		if err := n.serfAgent.Start(); err != nil {
			can()
			n.logger.Error("serf agent failed to start", zap.Error(err))
			return err
		}
		n.logger.Info("serf agent started", zap.Bool("isSeed", n.config.IsSerfSeed), zap.String("node-name", n.serfAgent.SerfConfig().NodeName))
		if !n.config.IsSerfSeed {
			n.logger.Info("joining serf cluster using", zap.Strings("peers", n.config.SerfJoinAddrs))
			const replay = false
			n.serfAgent.Join(n.config.SerfJoinAddrs, replay)
		}

		// Create a new consistent instance
		cfg := consistent.Config{
			PartitionCount:    numShards,
			ReplicationFactor: 20,
			Load:              1.25,
			Hasher:            hasher{},
		}
		serfMembers := n.serfAgent.Serf().Members()
		peers := []consistent.Member{}
		for _, member := range serfMembers {
			if member.Status != serf.StatusAlive {
				continue
			}
			peers = append(peers, myMember(member.Name))
		}

		c := consistent.New(peers, cfg)
		n.consistent = c

		<-n.serfAgent.ShutdownCh() // wait for the serf agent to shutdown
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
		return n.serfAgent.Shutdown()
	})

	// Go routine to cleanup raft agent on shutdown
	g.Go(func() error {
		<-ctx.Done()
		n.logger.Info("Stopping raft agent")
		n.nh.Close()
		return nil
	})

	n.logger.Info("Server started")
	if err := g.Wait(); err != nil {
		n.logger.Warn("Child workers returned an error", zap.Error(err))
		return err
	} else {
		n.logger.Info("Clean shutdown")
	}
	return nil
}
