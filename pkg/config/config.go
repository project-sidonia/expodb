package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	multierror "github.com/hashicorp/go-multierror"
	template "github.com/hashicorp/go-sockaddr/template"
	flag "github.com/ogier/pflag"
)

type args struct {
	BindAddress          string
	JoinAddress          string
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

type Config struct {
	SerfBindAddress   string
	SerfBindPort      int
	SerfAdvertiseAddr string
	SerfAdvertisePort int
	SerfDataDir       string
	SerfJoinAddrs     []string
	IsSerfSeed        bool

	HTTPBindAddress string
	HTTPBindPort    int

	JoinAddress string // Deprecated ??

	RaftBindAddress string
	RaftBindPort    int
	RaftDataDir     string
	Bootstrap       bool

	NodeName string
}

func (c *Config) ID() string {
	return c.NodeName
}

type ConfigError struct {
	ConfigurationPoint string
	Err                error
}

func (err *ConfigError) Error() string {
	return fmt.Sprintf("%s: %s", err.ConfigurationPoint, err.Err.Error())
}

func LoadConfig() (*Config, error) {
	var errors *multierror.Error

	var args = getArgs()

	// Bind address
	var bindAddr net.IP
	resolvedBindAddr, err := template.Parse(args.BindAddress)
	if err != nil {
		configErr := &ConfigError{
			ConfigurationPoint: "bind-address",
			Err:                err,
		}
		errors = multierror.Append(errors, configErr)
	} else {
		bindAddr = net.ParseIP(resolvedBindAddr)
		if bindAddr == nil {
			err := fmt.Errorf("cannot parse IP address: %s", resolvedBindAddr)
			configErr := &ConfigError{
				ConfigurationPoint: "bind-address",
				Err:                err,
			}
			errors = multierror.Append(errors, configErr)
		}
	}

	// Raft port
	if args.RaftPort < 1 || args.RaftPort > 65536 {
		configErr := &ConfigError{
			ConfigurationPoint: "raft-port",
			Err:                fmt.Errorf("port numbers must be 1 < port < 65536, got:%v", args.SerfPort),
		}
		errors = multierror.Append(errors, configErr)
	}

	// Node Name
	if args.NodeName == "" {
		errors = multierror.Append(errors, fmt.Errorf("node-name must be set to 1-5"))
	}

	// Serf bind port
	if args.SerfPort < 1 || args.SerfPort > 65536 {
		configErr := &ConfigError{
			ConfigurationPoint: "serf-port",
			Err:                fmt.Errorf("port numbers must be 1 < port < 65536, got:%v", args.SerfPort),
		}
		errors = multierror.Append(errors, configErr)
	}

	// Serf advertise bind port
	var serfAdvertisePort int
	var serfAdvertiseAddress string
	if args.SerfAdvertisePort == 0 {
		serfAdvertisePort = args.SerfPort
	} else if args.SerfAdvertisePort < 0 || args.SerfAdvertisePort > 65536 {
		configErr := &ConfigError{
			ConfigurationPoint: "serf-advertise-port",
			Err:                fmt.Errorf("port numbers must be 1 < port < 65536, got:%v", args.SerfAdvertisePort),
		}
		errors = multierror.Append(errors, configErr)
	}
	serfAdvertiseAddress = bindAddr.String()
	if args.SerfAdvertiseAddress != "" {
		resolvedAdAddr, err := template.Parse(serfAdvertiseAddress)
		if err != nil {
			configErr := &ConfigError{
				ConfigurationPoint: "serf-advertise-address",
				Err:                err,
			}
			errors = multierror.Append(errors, configErr)
		} else {
			selfAdAddr := net.ParseIP(resolvedBindAddr)
			if selfAdAddr == nil {
				err := fmt.Errorf("cannot parse IP address: %s", resolvedAdAddr)
				configErr := &ConfigError{
					ConfigurationPoint: "serf-advertise-address",
					Err:                err,
				}
				errors = multierror.Append(errors, configErr)
			}
			serfAdvertiseAddress = selfAdAddr.String()
		}
	}

	// HTTP port
	if args.HTTPPort < 1 || args.HTTPPort > 65536 {
		configErr := &ConfigError{
			ConfigurationPoint: "http-port",
			Err:                fmt.Errorf("port numbers must be 1 < port < 65536"),
		}
		errors = multierror.Append(errors, configErr)
	}

	// Raft Data directory
	raftDataDir, err := filepath.Abs(args.RaftDataDir)
	if err != nil {
		configErr := &ConfigError{
			ConfigurationPoint: "raft-data-dir",
			Err:                err,
		}
		errors = multierror.Append(errors, configErr)
	}

	//Serf Data directory
	serfDataDir, err := filepath.Abs(args.SerfDataDir)
	if err != nil {
		configErr := &ConfigError{
			ConfigurationPoint: "serf-data-dir",
			Err:                err,
		}
		errors = multierror.Append(errors, configErr)
	}

	if !args.IsSeed && len(args.SerfJoinAddrs) == 0 {
		configErr := &ConfigError{
			ConfigurationPoint: "serf-join",
			Err:                fmt.Errorf("at least one serf join address is required or set --is-seed=true"),
		}
		errors = multierror.Append(errors, configErr)
	}

	if err := errors.ErrorOrNil(); err != nil {
		return nil, err
	}

	return &Config{
		NodeName:          args.NodeName,
		RaftDataDir:       raftDataDir,
		JoinAddress:       args.JoinAddress, //TODO - validate this looks address-like
		IsSerfSeed:        args.IsSeed,
		SerfDataDir:       serfDataDir,
		SerfBindAddress:   bindAddr.String(),
		SerfBindPort:      args.SerfPort,
		SerfAdvertiseAddr: serfAdvertiseAddress,
		SerfAdvertisePort: serfAdvertisePort,
		SerfJoinAddrs:     args.SerfJoinAddrs,
		RaftBindAddress:   bindAddr.String(),
		RaftBindPort:      args.RaftPort,
		HTTPBindAddress:   bindAddr.String(),
		HTTPBindPort:      args.HTTPPort,
		Bootstrap:         args.Bootstrap,
	}, nil
}

func getArgs() *args {
	var parsedArgs args

	pwd, err := os.Getwd()
	if err != nil {
		pwd = "."
	}

	defaultSerfDataPath := filepath.Join(pwd, "serf-default")
	flag.StringVarP(&parsedArgs.SerfDataDir, "serf-data-dir", "s",
		defaultSerfDataPath, "Path in which to store Serf data")

	defaultRaftDataPath := filepath.Join(pwd, "raft-default")
	flag.StringVarP(&parsedArgs.RaftDataDir, "raft-data-dir", "d",
		defaultRaftDataPath, "Path in which to store Raft data")

	flag.StringVarP(&parsedArgs.BindAddress, "bind-address", "a",
		"127.0.0.1", "IP Address on which to bind")
	flag.IntVarP(&parsedArgs.SerfPort, "serf-port", "S",
		6000, "Port on which to bind serf")
	flag.StringVarP(&parsedArgs.SerfAdvertiseAddress, "advertise-address", "A",
		"", "IP Address on which to advertise to other members of the ")
	flag.IntVarP(&parsedArgs.SerfAdvertisePort, "serf-advertise-port", "T",
		0, "Port on which to advertise serf on")
	var serfJoinAddrsStr string
	flag.StringVarP(&serfJoinAddrsStr, "serf-join", "Z",
		"", "Comma seperated list of addresses to serf brokers to join at start time.  Required to be one or more.")

	flag.IntVarP(&parsedArgs.RaftPort, "raft-port", "R",
		7000, "Port on which to bind Raft")

	flag.IntVarP(&parsedArgs.HTTPPort, "http-port", "H",
		8000, "Port on which to bind HTTP")

	flag.StringVar(&parsedArgs.JoinAddress, "join",
		"", "Address of another node to join")

	flag.StringVarP(&parsedArgs.NodeName, "node-name", "N",
		"",
		"the name to use for this node when gossiping.  most be uniq.  Defaults to base64(raft.Address)")

	flag.BoolVar(&parsedArgs.Bootstrap, "bootstrap",
		false, "Bootstrap the cluster with this node")

	flag.BoolVar(&parsedArgs.IsSeed, "is-seed",
		false, "configure as the first node in the cluster, no serf join addresses required.")

	flag.Parse()

	parsedArgs.SerfJoinAddrs = strings.Split(serfJoinAddrsStr, ",")

	return &parsedArgs
}
