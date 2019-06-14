package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	multierror "github.com/hashicorp/go-multierror"
	template "github.com/hashicorp/go-sockaddr/template"
	flag "github.com/ogier/pflag"
)

type args struct {
	BindAddress string
	JoinAddress string
	RaftPort    int
	SerfPort    int
	HTTPPort    int
	SerfDataDir string
	RaftDataDir string
	Bootstrap   bool
	LoggerName  string
}

type Config struct {
	SerfAddress net.Addr
	RaftAddress net.Addr
	HTTPAddress net.Addr
	JoinAddress string
	SerfDataDir string
	RaftDataDir string
	Bootstrap   bool
	LoggerName  string
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
			Err:                fmt.Errorf("port numbers must be 1 < port < 65536"),
		}
		errors = multierror.Append(errors, configErr)
	}
	// Construct Raft Address
	raftAddr := &net.TCPAddr{
		IP:   bindAddr,
		Port: args.RaftPort,
	}

	// Serf port
	if args.SerfPort < 1 || args.SerfPort > 65536 {
		configErr := &ConfigError{
			ConfigurationPoint: "serf-port",
			Err:                fmt.Errorf("port numbers must be 1 < port < 65536"),
		}
		errors = multierror.Append(errors, configErr)
	}
	// Construct Serf Address
	serfAddr := &net.TCPAddr{
		IP:   bindAddr,
		Port: args.SerfPort,
	}

	// HTTP port
	if args.HTTPPort < 1 || args.HTTPPort > 65536 {
		configErr := &ConfigError{
			ConfigurationPoint: "http-port",
			Err:                fmt.Errorf("port numbers must be 1 < port < 65536"),
		}
		errors = multierror.Append(errors, configErr)
	}
	// Construct HTTP Address
	httpAddr := &net.TCPAddr{
		IP:   bindAddr,
		Port: args.HTTPPort,
	}

	// Raft Data directory
	raftDataDir, err := filepath.Abs(args.RaftDataDir)
	if err != nil {
		configErr := &ConfigError{
			ConfigurationPoint: "data-dir",
			Err:                err,
		}
		errors = multierror.Append(errors, configErr)
	}

	//Serf Data directory
	serfDataDir, err := filepath.Abs(args.SerfDataDir)
	if err != nil {
		configErr := &ConfigError{
			ConfigurationPoint: "data-dir",
			Err:                err,
		}
		errors = multierror.Append(errors, configErr)
	}

	if err := errors.ErrorOrNil(); err != nil {
		return nil, err
	}

	return &Config{
		RaftDataDir: raftDataDir,
		SerfDataDir: serfDataDir,
		JoinAddress: args.JoinAddress, //TODO - validate this looks address-like
		RaftAddress: raftAddr,
		SerfAddress: serfAddr,
		HTTPAddress: httpAddr,
		Bootstrap:   args.Bootstrap,
	}, nil
}

func getArgs() *args {
	var parsedArgs args

	pwd, err := os.Getwd()
	if err != nil {
		pwd = "."
	}

	defaultSerfDataPath := filepath.Join(pwd, "serf")
	flag.StringVarP(&parsedArgs.SerfDataDir, "serf-data-dir", "s",
		defaultSerfDataPath, "Path in which to store Serf data")

	defaultRaftDataPath := filepath.Join(pwd, "raft")
	flag.StringVarP(&parsedArgs.RaftDataDir, "raft-data-dir", "d",
		defaultRaftDataPath, "Path in which to store Raft data")

	flag.StringVarP(&parsedArgs.BindAddress, "bind-address", "a",
		"127.0.0.1", "IP Address on which to bind")

	flag.IntVarP(&parsedArgs.RaftPort, "serf-port", "S",
		6000, "Port on which to bind serf")

	flag.IntVarP(&parsedArgs.RaftPort, "raft-port", "R",
		7000, "Port on which to bind Raft")

	flag.IntVarP(&parsedArgs.HTTPPort, "http-port", "H",
		8000, "Port on which to bind HTTP")

	flag.StringVar(&parsedArgs.JoinAddress, "join",
		"", "Address of another node to join")

	flag.StringVar(&parsedArgs.LoggerName, "log-name",
		"", "the prefix context to add to each log line")

	flag.BoolVar(&parsedArgs.Bootstrap, "bootstrap",
		false, "Bootstrap the cluster with this node")

	flag.Parse()
	return &parsedArgs
}
