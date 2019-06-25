package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/epsniff/expodb/pkg/server/agents/raft/machines/keyvalstore"
	"github.com/justinas/alice"
	"go.uber.org/zap"
)

type httpServer struct {
	address net.Addr
	node    *server
	logger  *zap.Logger
}

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
		server.handleRequest(w, r)
		//  } else if strings.HasPrefix(r.URL.Path, "/join") {
		//	   server.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (server *httpServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		server.handleKeyPost(w, r)
		return
	case http.MethodGet:
		server.handleKeyGet(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func removeKeyPath(s string) string {
	return strings.Replace(s, "/key", "", 1) // remove path so we can read URL
}

func (server *httpServer) handleKeyPost(w http.ResponseWriter, r *http.Request) {
	request := struct {
		Value string `json:"value"`
	}{}

	key := removeKeyPath(r.URL.Path)

	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		server.logger.Error("Bad request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := server.node.SetKeyVal(key, request.Value); err != nil {
		server.logger.Error("Failed to set keyvalue", zap.Error(err))
		statusInternalError(w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (server *httpServer) handleKeyGet(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	key := removeKeyPath(r.URL.Path)
	val, err := server.node.GetKeyVal(key)
	if err == keyvalstore.ErrKeyNotFound {
		statusNotFound(w)
		return
	}
	response := struct {
		Value    string `json:"value"`
		IsLeader bool   `json:"leader"`
		Nodes    string `json:"nodes"`
	}{
		Value:    val,
		IsLeader: server.node.raftAgent.IsLeader(), // just for debugging
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		server.logger.Error("Failed to marshal response", zap.Error(err))
		statusInternalError(w)
		return
	}

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
