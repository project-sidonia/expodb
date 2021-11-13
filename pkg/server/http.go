package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/epsniff/expodb/pkg/server/state-machines/keyvalstore"
	"github.com/justinas/alice"
	"go.uber.org/zap"
)

type httpServer struct {
	address net.Addr
	node    *server
	logger  *zap.Logger
}

// Start starts the http server
func (server *httpServer) Start() {
	server.logger.Info("Starting http server", zap.String("address", server.address.String()))
	// .Str("address", server.address.String()).Msg()
	c := alice.New()
	handler := c.Then(server)

	if err := http.ListenAndServe(server.address.String(), handler); err != nil {
		server.logger.Fatal("Error running HTTP server", zap.Error(err))
	}
}

func (server *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		server.handleKeyRequest(w, r)
		//	} else if strings.HasPrefix(r.URL.Path, "/join") {
		//		server.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (server *httpServer) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		switch {
		case strings.Contains(r.URL.Path, "/_update"):
			server.handleKeyUpdate(w, r)
		case strings.Contains(r.URL.Path, "/_fetch"):
			server.handleKeyFetch(w, r)
		case strings.Contains(r.URL.Path, "/_query"):
			server.handleKeyQuery(w, r)
		}
		return
	case http.MethodGet:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

func (server *httpServer) handleKeyUpdate(w http.ResponseWriter, r *http.Request) {
	req := struct {
		Table  string `json:"table"`
		RowKey string `json:"key"`
		Column string `json:"column"`
		Value  string `json:"value"`
	}{}

	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		server.logger.Error("Bad request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := server.node.SetKeyVal(req.Table, req.RowKey, req.Column, req.Value); err != nil {
		server.logger.Error("Failed to set keyvalue", zap.Error(err))
		statusInternalError(w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (server *httpServer) handleKeyFetch(w http.ResponseWriter, r *http.Request) {

	req := struct {
		Table string `json:"table"`
		Key   string `json:"key"`
	}{}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		server.logger.Error("Bad request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	val, err := server.node.GetByRowKey(req.Table, req.Key)
	if err == keyvalstore.ErrKeyNotFound {
		statusNotFound(w)
		return
	} else if err != nil {
		server.logger.Error("Failed to get key from statemachine", zap.Error(err))
		statusInternalError(w)
		return
	}
	response := struct {
		Result   map[string]string `json:"result"`
		IsLeader bool              `json:"leader"`
		// Nodes    string            `json:"nodes"`
	}{
		Result:   val,
		IsLeader: server.node.raftAgent.IsLeader(), // just for debugging
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		server.logger.Error("Failed to marshal response", zap.Error(err))
		statusInternalError(w)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(responseBytes)
}

func (server *httpServer) handleKeyQuery(w http.ResponseWriter, r *http.Request) {

	req := struct {
		Table string `json:"table"`
		Query string `json:"query"`
	}{}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		server.logger.Error("Bad request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	vals, err := server.node.GetByRowByQuery(req.Table, req.Query)
	if err == keyvalstore.ErrKeyNotFound {
		statusNotFound(w)
		return
	} else if err != nil {
		server.logger.Error("Failed to query statemachine", zap.Error(err))
		statusInternalError(w)
		return
	}
	response := struct {
		Results  []map[string]string `json:"results"`
		IsLeader bool                `json:"leader"`
		// Nodes    string            `json:"nodes"`
	}{
		Results:  vals,
		IsLeader: server.node.raftAgent.IsLeader(), // just for debugging
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		server.logger.Error("Failed to marshal response", zap.Error(err))
		statusInternalError(w)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(responseBytes)
}

// func (server *httpServer) handleJoin(w http.ResponseWriter, r *http.Request) {
// 	peerAddress := r.Header.Get("Peer-Address")
// 	if peerAddress == "" {
// 		server.logger.Error("Peer-Address not set on request")
// 		w.WriteHeader(http.StatusBadRequest)
// 	}
//
// 	addPeerFuture := server.node.raftNode.AddVoter(
// 		raft.ServerID(peerAddress), raft.ServerAddress(peerAddress), 0, 0)
// 	if err := addPeerFuture.Error(); err != nil {
// 		server.logger.Error("Error joining peer to Raft", zap.String("peer.remoteaddr", peerAddress), zap.Error// (err))
// 		statusInternalError(w)
// 		return
// 	}
//
// 	server.logger.Info("Peer joined Raft", zap.String("peer.remoteaddr", peerAddress))
// 	w.WriteHeader(http.StatusOK)
// }

//~~~~~~~~~~~ Http Utils ~~~~~~~~~~~~~~~~~~~~~
func statusNotFound(w http.ResponseWriter) {
	status := http.StatusNotFound
	w.WriteHeader(status)
	fmt.Fprint(w, `{"status": "404 not found"}`)
}

func statusInternalError(w http.ResponseWriter) {
	status := http.StatusInternalServerError
	w.WriteHeader(status)
	fmt.Fprint(w, `{"status": "internal server error"}`)
}
