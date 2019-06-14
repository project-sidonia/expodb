package server

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type server struct {
	config   *config.Config
	raftNode *raft.Raft
	fsm      *fsm
	logger   *zap.Logger
}

func (n *server) IsLeader() bool {
	return n.raftNode.State() == raft.Leader
}

func (n *server) Serve() {

	// TODO move the Join logic into serf handler like nomad and consul
	if n.config.JoinAddress != "" {
		go func() {
			retryJoin := func() error {
				url := url.URL{
					Scheme: "http",
					Host:   n.config.JoinAddress,
					Path:   "join",
				}

				req, err := http.NewRequest(http.MethodPost, url.String(), nil)
				if err != nil {
					return err
				}
				req.Header.Add("Peer-Address", n.config.RaftAddress.String())

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
					n.logger.Error("Error joining cluster", zap.Error(err))
					time.Sleep(5 * time.Second)
				} else {
					break
				}
			}
		}()
	}

	httpServer := &httpServer{
		node:    n,
		address: n.config.HTTPAddress,
		logger:  n.logger.Named("http"),
	}

	httpServer.Start()
}
