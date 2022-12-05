package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/epsniff/expodb/pkg/config"
	"github.com/epsniff/expodb/pkg/server/state-machines/datastore"
	"go.uber.org/zap"
)

func TestSQLServer(t *testing.T) {

	var cfg = zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logger, err := cfg.Build()
	if err != nil {
		t.Logf("log config errors - %s\n", err)
		t.FailNow()
	}
	defer logger.Sync()

	args := config.DefaultArgs()
	args.IsSeed = true
	ecfg, err := config.LoadConfig(args)
	if err != nil {
		t.Logf("Configuration errors - %s\n", err)
		t.FailNow()
	}
	ecfg.UseInMemory = true

	logger = logger.Named(ecfg.ID())

	srv, err := New(ecfg, logger)
	if err != nil {
		t.Logf("Error configuring node: %s", err)
		t.FailNow()
	}
	go func() {
		if err := srv.Serve(context.TODO()); err != nil {
			t.FailNow()
		}
	}()

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// server.handleKeyUpdate(w, r)
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	req := struct {
		Table  string `json:"table"`
		RowKey string `json:"key"`
		Column string `json:"column"`
		Value  string `json:"value"`
	}{
		Table:  "test_tab",
		RowKey: "test_row",
		Column: "test_col",
		Value:  "test_val1",
	}

	if err := srv.SetKeyVal(req.Table, req.RowKey, req.Column, req.Value); err != nil {
		t.Logf("Failed to set keyvalue: %v", err)
		t.FailNow()
		return
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// server.handleKeyQuery(w, r)
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	req2 := struct {
		Table string `json:"table"`
		Query string `json:"query"`
	}{
		Table: "test_tab",
		Query: "SELECT * FROM test_tab",
	}

	vals, err := srv.GetByRowByQuery(req.Table, req2.Query)
	if err == datastore.ErrKeyNotFound {
		t.Logf("not found: %v", err)
		t.FailNow()
		return
	} else if err != nil {
		t.Logf("Failed to query keyvalue: %v", err)
		t.FailNow()
		return
	}
	response := struct {
		Results  []map[string]string `json:"results"`
		IsLeader bool                `json:"leader"`
		// Nodes    string            `json:"nodes"`
	}{
		Results:  vals,
		IsLeader: srv.raftAgent.IsLeader(), // just for debugging
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		t.Logf("Failed to json marshal: %v", err)
		t.FailNow()
		return
	}

	fmt.Printf("response: %s", responseBytes)
}
