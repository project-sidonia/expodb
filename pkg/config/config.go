package config

import (
	"encoding/base64"
	"fmt"
	"net"
	"path/filepath"

	multierror "github.com/hashicorp/go-multierror"
	template "github.com/hashicorp/go-sockaddr/template"
)

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

	RaftBindAddress string
	RaftBindPort    int
	RaftDataDir     string
	Bootstrap       bool

	NodeName string

	UseInMemory bool // use in memory store for testing
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

func LoadConfig(args *Args) (*Config, error) {
	var errors *multierror.Error

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
		name := fmt.Sprintf("%s:%d", args.BindAddress, args.RaftPort)
		args.NodeName = base64.StdEncoding.EncodeToString([]byte(name))
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
		IsSerfSeed:        args.IsSeed,
		SerfDataDir:       serfDataDir,
		SerfBindAddress:   bindAddr.String(),
		SerfBindPort:      args.SerfPort,
		SerfAdvertiseAddr: serfAdvertiseAddress,
		SerfAdvertisePort: serfAdvertisePort,
		SerfJoinAddrs:     args.SerfJoinAddrs,
		RaftBindAddress:   bindAddr.String(),
		RaftBindPort:      args.RaftPort,

		HTTPBindAddress: bindAddr.String(),
		HTTPBindPort:    args.HTTPPort,
		Bootstrap:       args.Bootstrap,
	}, nil
}
