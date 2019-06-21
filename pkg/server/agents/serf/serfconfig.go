package serf

import (
	"fmt"
	"time"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/epsniff/expodb/pkg/version"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

func createSerfConfig(config *config.Config, logger *zap.Logger, ch chan serf.Event, snapshotPath string) (*serf.Config, error) {

	serfConfig := serf.DefaultConfig()
	serfConfig.Init()

	// TODO someday support node to node encrypted traffic
	//  encryptKey, err := config.EncryptBytes()
	//  if err != nil {
	//  	c.Ui.Error(fmt.Sprintf("Invalid encryption key: %s", err))
	//  	return nil
	//  }
	//  serfConfig.MemberlistConfig.SecretKey = encryptKey

	// ver 5 is the serf.ProtocolVersionMax at the time of this writing
	serfConfig.ProtocolVersion = 5
	// LeavePropagateDelay is used to make sure broadcasted leave intents propagate
	// This value was tuned using https://www.serf.io/docs/internals/simulator.html to
	// allow for convergence in 99.9% of nodes in a 10 node cluster
	serfConfig.LeavePropagateDelay = 1 * time.Second
	serfConfig.SnapshotPath = snapshotPath
	serfConfig.CoalescePeriod = 3 * time.Second
	serfConfig.QuiescentPeriod = time.Second
	serfConfig.Logger = zap.NewStdLog(logger)
	serfConfig.MemberlistConfig.LogOutput = nil
	serfConfig.LogOutput = nil

	serfConfig.EventCh = ch // setup the shared channel

	// TODO check if this should be set after Init?
	slogger := logger.Named("serf")
	serfConfig.MemberlistConfig.Logger = zap.NewStdLog(slogger.Named("memberlist"))
	// TODO someday support configs optimized for other network types.
	// 	serfConfig.MemberlistConfig = memberlist.DefaultWANConfig()
	//	serfConfig.MemberlistConfig = memberlist.DefaultLocalConfig()
	serfConfig.MemberlistConfig = memberlist.DefaultLANConfig()
	serfConfig.MemberlistConfig.BindAddr = config.SerfBindAddress
	serfConfig.MemberlistConfig.BindPort = config.SerfBindPort
	serfConfig.MemberlistConfig.AdvertiseAddr = config.SerfAdvertiseAddr
	serfConfig.MemberlistConfig.AdvertisePort = config.SerfAdvertisePort
	serfConfig.MemberlistConfig.EnableCompression = true

	serfConfig.NodeName = config.NodeName
	serfConfig.Tags["role"] = "expodb"
	//serfConfig.Tags["region"] = s.config.Region
	//serfConfig.Tags["dc"] = s.config.Datacenter
	serfConfig.Tags["ver"] = fmt.Sprintf("%s", version.ServerVersion)
	//serfConfig.Tags["build"] = s.config.Build
	//serfConfig.Tags["raft_vsn"] = fmt.Sprintf("%d", s.config.RaftConfig.ProtocolVersion)
	// serfConfig.Tags["id"] = s.config.NodeID
	// serfConfig.Tags["http_addr"] =
	// serfConfig.Tags["http_port"] =
	// serfConfig.Tags["raft_addr"] =
	// serfConfig.Tags["raft_port"] =
	// serfConfig.Tags["serf_addr"] =
	// serfConfig.Tags["serf_port"] =
	// serfConfig.Tags["grpc_addr"] = // Address that clients will use to RPC to servers
	// serfConfig.Tags["grpc_port"] = // Port servers use to RPC to one and another
	if config.Bootstrap {
		serfConfig.Tags["bootstrap"] = "1"
	}
	// TODO add support so the leader can determine when to add new members to raft as
	// a voting member.  Currently all new members are added to raft too.
	// if s.config.NonVoter {
	// 	serfConfig.Tags["nonvoter"] = "1"
	// }

	// TODO figure out if we need this?  I saw it in the nomad repo.
	// serfConfig.Merge = &serfMergeDelegate{}

	return serfConfig, nil
}
