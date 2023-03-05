package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/epsniff/expodb/pkg/server/interfaces"
	"github.com/epsniff/expodb/pkg/state-machines/kvstore"
	"github.com/justinas/alice"
	"go.uber.org/zap"
)

type HttpServer struct {
	address net.Addr
	kvstore interfaces.KeyValueStore
	logger  *zap.Logger
}

func New(address net.Addr, kvstore interfaces.KeyValueStore, logger *zap.Logger) *HttpServer {
	return &HttpServer{
		address: address,
		kvstore: kvstore,
		logger:  logger,
	}
}

// Start starts the http server
func (server *HttpServer) Start(ctx context.Context) error {
	server.logger.Info("Starting http server", zap.String("address", server.address.String()))
	// .Str("address", server.address.String()).Msg()
	c := alice.New()
	handler := c.Then(server)

	if err := http.ListenAndServe(server.address.String(), handler); err != nil {
		server.logger.Fatal("Error running HTTP server", zap.Error(err))
	}
	return nil
}

func (server *HttpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/_diag") {
		server.handleKeyRequest(w, r)
		//	} else if strings.HasPrefix(r.URL.Path, "/join") {
		//		server.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (server *HttpServer) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
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

func (server *HttpServer) handleKeyUpdate(w http.ResponseWriter, r *http.Request) {
	req := struct {
		Namespace string `json:"namespace"`
		RowKey    string `json:"key"`
		Value     string `json:"value"`
	}{}

	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		server.logger.Error("Bad request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := server.kvstore.StoreSingleValue(req.Namespace, req.RowKey, req.Value); err != nil {
		server.logger.Error("Failed to set keyvalue", zap.Error(err))
		statusInternalError(w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (server *HttpServer) handleKeyFetch(w http.ResponseWriter, r *http.Request) {

	req := struct {
		Namespace string `json:"namespace"`
		Key       string `json:"key"`
	}{}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		server.logger.Error("Bad request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	val, err := server.kvstore.RetrieveSingleValue(req.Namespace, req.Key)
	if err == kvstore.ErrKeyNotFound {
		statusNotFound(w)
		return
	} else if err != nil {
		server.logger.Error("Failed to get key from statemachine", zap.Error(err))
		statusInternalError(w)
		return
	}
	response := struct {
		Result string `json:"result"`
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
