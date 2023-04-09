package multiraft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	machines "github.com/epsniff/expodb/pkg/server/state-machines"
	"github.com/epsniff/expodb/pkg/server/state-machines/simplestore"
	"github.com/hashicorp/raft"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// fsm implements the raft.FSM interface and is implemented by clients to make use of the replicated log.
type fsm struct {
	fsmProvider machines.StateMachine

	ShardID   uint64
	ReplicaID uint64
	Count     uint64
}

// NewExampleStateMachine creates and return a new ExampleStateMachine object.
func NewFSM(shardID uint64, replicaID uint64) sm.IStateMachine {
	raftKvpStore := simplestore.New()
	f := &fsm{
		fsmProvider: raftKvpStore,
		ShardID:     shardID,
		ReplicaID:   replicaID,
		Count:       0,
	}
	return f
}

// Apply implements the raft.FSM.Apply interface and applies a Raft log entry
// to the key-value store.
// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (fsm *fsm) Apply(logEntry *raft.Log) interface{} {
	res, err := fsm.fsmProvider.Apply(logEntry.Data)
	if err != nil {
		return fmt.Errorf("failed to apply raft log to finite state machines: %w", err)
	}
	return res
}

// Lookup performs local lookup on the fsm instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *fsm) Lookup(e interface{}) (interface{}, error) {
	return s.fsmProvider.Lookup(e)
}

// Update updates the object using the specified committed raft entry.
func (s *fsm) Update(e sm.Entry) (sm.Result, error) {
	resp, err := s.fsmProvider.Apply(e.Cmd)
	if err != nil {
		return sm.Result{Value: simplestore.ResultCodeFailure}, err
	}
	return sm.Result{
		Value: simplestore.ResultCodeSuccess,
		Data:  resp.([]byte),
	}, nil
}

func (fsm *fsm) PrepareSnapshot() (ctx interface{}, err error) {
	return
}

func (f *fsm) SaveSnapshot(w io.Writer, sfc sm.ISnapshotFileCollection, stopc <-chan struct{}) (err error) {
	state, err := f.fsmProvider.Persist()
	if err != nil {
		return fmt.Errorf("failed to get snapshots from finite state machines: %w", err)
	}
	b, err := json.Marshal(state)
	if err == nil {
		_, err = io.Copy(w, bytes.NewReader(b))
	}

	return
}

func (f *fsm) RecoverFromSnapshot(r io.Reader, sfc []sm.SnapshotFile, stopc <-chan struct{}) (err error) {
	var state []byte
	if err := json.NewDecoder(r).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	if err := f.fsmProvider.Restore(state); err != nil {
		return fmt.Errorf("failed to get restore snapshots for finite state machines: %w", err)
	}
	return nil
}

func (f *fsm) Close() (err error) {
	return
}
