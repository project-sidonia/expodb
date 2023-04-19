package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/epsniff/expodb/pkg/config"
	"github.com/epsniff/expodb/pkg/server/agents/multiraft"
	serfagent "github.com/epsniff/expodb/pkg/server/agents/serf"
	machines "github.com/epsniff/expodb/pkg/server/state-machines"
	"github.com/epsniff/expodb/pkg/server/state-machines/simplestore"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v4"
	dgConfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	shardID1  uint64 = 0
	numShards int    = 5
)

type server struct {
	config *config.Config
	logger *zap.Logger

	metadata *metadata

	serfAgent *serfagent.Agent

	raftNotifyCh chan bool
	nh           *dragonboat.NodeHost

	raftAgentsMu sync.Mutex
	raftAgents   map[uint64]raftAgent

	consistent *consistent.Consistent

	replicaID uint64
}

type raftAgent interface {
	AddVoter(replicaID uint64, peerAddress string) error
	Apply(val machines.RaftEntry) error
	Read(query interface{}) (interface{}, error)
	IsLeader() (bool, error)
	//LeaderAddress() string
	Shutdown() error
}

// GetByRowKey gets a value from the raft key value fsm
func (n *server) GetByRowKey(table, rowKey string) (map[string]string, error) {
	query := simplestore.Query{
		Table:  table,
		RowKey: rowKey,
	}
	val, err := n.raftAgents[shardID1].Read(query)
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
func (n *server) SetKeyVal(table, key, col, val string) error {
	//kve := simplestore.NewKeyValEvent(simplestore.UpdateRowOp, table, col, key, val)
	kve := multiraft.KVData{Key: table + ":" + key + ":" + col, Val: val}
	return n.raftAgents[shardID1].Apply(kve)
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

		serfAgent:    serfAgent,
		raftNotifyCh: make(chan bool, 1),
		replicaID:    replicaID,

		raftAgents: map[uint64]raftAgent{},
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
	if config.Bootstrap {
		if err := ser.NewShard(config.Bootstrap, shardID1); err != nil {
			return nil, fmt.Errorf("creating shard: %w", err)
		}
	}

	// register ourselfs as a handler for serf events. See (n *server) HandleEvent(e serf.Event)
	serfAgent.RegisterEventHandler(ser)

	return ser, nil
}

func (n *server) NewShard(bootstrap bool, shardID uint64) error {
	members := map[uint64]string{}
	if bootstrap {
		members[n.replicaID] = n.nh.RaftAddress()
	}
	shardAgent, err := multiraft.New(n.nh, n.replicaID, shardID, members)
	if err != nil {
		return fmt.Errorf("creating raft agent: %w", err)
	}

	n.raftAgentsMu.Lock()
	defer n.raftAgentsMu.Unlock()
	n.raftAgents = map[uint64]raftAgent{
		shardID1: shardAgent,
	}
	return nil
}

func (n *server) LeaderUpdated(info raftio.LeaderInfo) {
	// TODO (ajr) When we have actual multiraft!
	if info.ShardID == shardID1 {
		n.raftNotifyCh <- info.LeaderID == n.replicaID
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		// for i := 1; i < numShards+1; i++ {
		if len(n.consistent.GetMembers()) < 3 {
			n.logger.Warn("Not enough members to schedule shards", zap.Int("num_members", len(n.consistent.GetMembers())))
			return nil
		}
		members, err := n.consistent.GetClosestNForPartition(int(shardID1), 3)
		if err != nil {
			return fmt.Errorf("getting closest members: %w", err)
		}
		for _, member := range members {
			// Handle removing ourselves from shards we're not a part of
			if n.config.ID() == member.String() && n.raftAgents[uint64(shardID1)] == nil {
				// We are the closest member to this shard, so we should schedule it.
				if err := n.NewShard(false, uint64(shardID1)); err != nil {
					return fmt.Errorf("creating shard: %w", err)
				}
				break
			}
			for _, shardAgent := range n.raftAgents {
				isLeader, err := shardAgent.IsLeader()
				if err != nil {
					return fmt.Errorf("checking if leader: %w", err)
				}
				if !isLeader {
					continue
				}

				// TODO check bool for error
				nodedata, _ := n.metadata.FindByID(member.String())
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
				err = n.raftAgents[shardID1].AddVoter(replicaID, nodedata.RaftAddr())
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
	}
	timer.Reset(30 * time.Second)
	return nil
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
		var errors *multierror.Error
		for _, agent := range n.raftAgents {
			err := agent.Shutdown()
			if err != nil {
				err = multierror.Append(errors, err)
			}
		}
		return errors.ErrorOrNil()
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
