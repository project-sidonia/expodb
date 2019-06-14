package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	defer logger.Sync()
	config, err := argsToConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration errors - %s\n", err)
		os.Exit(1)
	}
	logger = logger.Named(config.LoggerName)

	serf.DefaultConfig()

	node, err := NewNode(config, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring node: %s", err)
		os.Exit(1)
	}

	if config.JoinAddress != "" {
		go func() {
			retryJoin := func() error {
				url := url.URL{
					Scheme: "http",
					Host:   config.JoinAddress,
					Path:   "join",
				}

				req, err := http.NewRequest(http.MethodPost, url.String(), nil)
				if err != nil {
					return err
				}
				req.Header.Add("Peer-Address", config.RaftAddress.String())

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return err
				}

				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("non 200 status code: %d", resp.StatusCode)
				}

				return nil
			}

			for {
				if err := retryJoin(); err != nil {
					logger.Error("Error joining cluster", zap.Error(err))
					time.Sleep(5 * time.Second)
				} else {
					break
				}
			}
		}()
	}

	httpServer := &httpServer{
		node:    node,
		address: config.HTTPAddress,
		logger:  logger.Named("http"),
	}

	httpServer.Start()

}

type node struct {
	config   *Config
	raftNode *raft.Raft
	fsm      *fsm
	log      *zap.Logger
}

func (n *node) IsLeader() bool {
	return n.raftNode.State() == raft.Leader
}

func NewNode(config *Config, logger *zap.Logger) (*node, error) {
	fsm := &fsm{
		stateValue: map[string]int{},
		log:        logger,
	}

	if err := os.MkdirAll(config.RaftDataDir, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.SerfDataDir, 0700); err != nil {
		return nil, err
	}
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.RaftAddress.String())
	raftConfig.Logger = NewHclog2ZapLogger(logger)
	transportLogger := logger.Named("raft.transport")
	transport, err := raftTransport(config.RaftAddress, &loggerWriter{transportLogger})
	if err != nil {
		return nil, err
	}

	snapshotStoreLogger := logger.Named("raft.snapshots")
	snapshotStore, err := raft.NewFileSnapshotStore(config.RaftDataDir, 1, &loggerWriter{snapshotStoreLogger})
	if err != nil {
		return nil, err
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftDataDir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftDataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}

	logger.Info("NewNode created raft node", zap.String("raft-config", fmt.Sprintf("%+v", raftConfig)))

	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore,
		snapshotStore, transport)
	if err != nil {
		return nil, err
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
	return &node{
		config:   config,
		raftNode: raftNode,
		log:      logger,
		fsm:      fsm,
	}, nil
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
