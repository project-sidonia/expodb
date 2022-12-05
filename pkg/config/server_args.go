package config

import (
	"os"
	"path/filepath"
	"strings"

	flag "github.com/ogier/pflag"
)

type Args struct {
	BindAddress          string
	RaftPort             int
	SerfPort             int
	SerfAdvertiseAddress string
	SerfAdvertisePort    int
	SerfJoinAddrs        []string
	HTTPPort             int
	SerfDataDir          string
	RaftDataDir          string
	Bootstrap            bool
	IsSeed               bool
	NodeName             string
}

var pwd string

func init() {
	var err error
	pwd, err = os.Getwd()
	if err != nil {
		panic(err)
	}
}

var (
	defaultNodeName = "default"

	defaultSerfDataPath = filepath.Join(pwd, "serf-default")
	defaultRaftDataPath = filepath.Join(pwd, "raft-default")

	defaultBindAddress          = "127.0.0.1"
	defaultSerfPort             = 6000
	defaultSerfAdvertiseAddress = ""
	defaultSerfAdvertisePort    = 0
	defaultSerfJoinAddrs        = []string{}

	defaultRaftPort = 7000
	defaultHTTPPort = 8000

	defaultBootstrap = false
	defaultIsSeed    = false
)

func DefaultArgs() *Args {
	return &Args{
		BindAddress:          defaultBindAddress,
		RaftPort:             defaultRaftPort,
		SerfPort:             defaultSerfPort,
		SerfAdvertiseAddress: defaultSerfAdvertiseAddress,
		SerfAdvertisePort:    defaultSerfAdvertisePort,
		SerfJoinAddrs:        defaultSerfJoinAddrs,
		HTTPPort:             defaultHTTPPort,
		SerfDataDir:          defaultSerfDataPath,
		RaftDataDir:          defaultRaftDataPath,
		Bootstrap:            defaultBootstrap,
		IsSeed:               defaultIsSeed,
		NodeName:             defaultNodeName,
	}
}

func ParseArgs() *Args {
	var parsedArgs = DefaultArgs()

	flag.StringVarP(&parsedArgs.SerfDataDir, "serf-data-dir", "s",
		defaultSerfDataPath, "Path in which to store Serf data")
	flag.StringVarP(&parsedArgs.RaftDataDir, "raft-data-dir", "d",
		defaultRaftDataPath, "Path in which to store Raft data")
	flag.StringVarP(&parsedArgs.BindAddress, "bind-address", "a",
		defaultBindAddress, "IP Address on which to bind")
	flag.IntVarP(&parsedArgs.SerfPort, "serf-port", "S",
		defaultSerfPort, "Port on which to bind serf")
	flag.StringVarP(&parsedArgs.SerfAdvertiseAddress, "advertise-address", "A",
		defaultSerfAdvertiseAddress, "IP Address on which to advertise to other members of the ")
	flag.IntVarP(&parsedArgs.SerfAdvertisePort, "serf-advertise-port", "T",
		defaultSerfAdvertisePort, "Port on which to advertise serf on")
	var serfJoinAddrsStr string
	flag.StringVarP(&serfJoinAddrsStr, "serf-join", "Z",
		"", "Comma separated list of addresses to serf brokers to join at start time.  Required to be one or more.")
	flag.IntVarP(&parsedArgs.RaftPort, "raft-port", "R",
		defaultRaftPort, "Port on which to bind Raft")
	flag.IntVarP(&parsedArgs.HTTPPort, "http-port", "H",
		defaultHTTPPort, "Port on which to bind HTTP")
	flag.StringVarP(&parsedArgs.NodeName, "node-name", "N",
		defaultNodeName,
		"the name to use for this node when gossiping.  most be uniq.  Defaults to base64(raft.Address)")
	flag.BoolVar(&parsedArgs.Bootstrap, "bootstrap",
		defaultBootstrap, "Bootstrap the cluster with this node")
	flag.BoolVar(&parsedArgs.IsSeed, "is-seed",
		defaultIsSeed, "configure as the first node in the cluster, no serf join addresses required.")

	flag.Parse()

	parsedArgs.SerfJoinAddrs = strings.Split(serfJoinAddrsStr, ",")

	return parsedArgs
}
