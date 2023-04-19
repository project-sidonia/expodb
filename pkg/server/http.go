package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/project-sidonia/expodb/pkg/server/machines"
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
	c := alice.New()
	handler := c.Then(server)

	if err := http.ListenAndServe(server.address.String(), handler); err != nil {
		server.logger.Fatal("Error running HTTP server", zap.Error(err))
	}
}

func (server *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		server.handleKeyRequest(w, r)
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
	if err == machines.ErrKeyNotFound {
		statusNotFound(w)
		return
	} else if err != nil {
		server.logger.Error("Failed to get key from statemachine", zap.Error(err))
		statusInternalError(w)
		return
	}
	response := struct {
		Result map[string]string `json:"result"`
	}{
		Result: val,
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

// ~~~~~~~~~~~ Http Utils ~~~~~~~~~~~~~~~~~~~~~
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
