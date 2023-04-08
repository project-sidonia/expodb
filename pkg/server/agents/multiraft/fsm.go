package multiraft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	machines "github.com/epsniff/expodb/pkg/server/state-machines"
	"github.com/epsniff/expodb/pkg/server/state-machines/simplestore"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// fsm implements the raft.FSM interface and is implemented by clients to make use of the replicated log.
type fsm struct {
	logger *zap.Logger

	fsmProvider machines.FSMProvider

	ShardID   uint64
	ReplicaID uint64
	Count     uint64
}

type Query struct {
	Key      uint16
	FSMQuery interface{}
}

// NewExampleStateMachine creates and return a new ExampleStateMachine object.
func NewFSM(shardID uint64, replicaID uint64) sm.IStateMachine {
	return &fsm{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Count:     0,
	}
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
	res, err := fsm.fsmProvider.Apply(logEntry)
	if err != nil {
		fsm.logger.Error("Failed to apply raft log to finite state machines: err:%v", zap.Error(err))
		return err
	}
	return res
}

// Snapshot implements the raft.FSM.Snapshot interface and returns a snapshot of all the
// underlying finite state machines.
// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running. Generally this means Snapshot should
// only capture a pointer to the state, and any expensive IO should happen
// as part of FSMSnapshot.Persist.
//
// Apply and Snapshot are always called from the same thread, but Apply will
// be called concurrently with FSMSnapshot.Persist. This means the FSM should
// be implemented to allow for concurrent updates while a snapshot is happening.
func (fsm *fsm) Snapshot() (raft.FSMSnapshot, error) {
	state, err := fsm.fsmProvider.SnapshotAll()
	if err != nil {
		fsm.logger.Error("Failed to get snapshots from finite state machines: err:%v", zap.Error(err))
		return nil, err
	}
	return &fsmSnapshot{State: state}, nil
}

// Restore implements the raft.FSM.Restore interface and restores the snapshot bytes from the
// collection of finite state machines.
// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (fsm *fsm) Restore(serialized io.ReadCloser) error {
	var snapshot fsmSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}
	err := fsm.fsmProvider.RestoreAll(snapshot.State)
	if err != nil {
		fsm.logger.Error("Failed to get restore snapshots for finite state machines: err:%v", zap.Error(err))
		return err
	}
	return nil
}

// fsmSnapshot Implements the raft.FSMSnapshot interface, which is returned by an FSM in
// response to a Snapshot.  It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
type fsmSnapshot struct {
	State map[uint16][]byte `json:"snapshot_state"`
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		snapshotBytes, err := json.Marshal(f)
		if err != nil {
			return err
		}

		if _, err := sink.Write(snapshotBytes); err != nil {
			return err
		}

		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is invoked when we are finished with the snapshot.
func (f *fsmSnapshot) Release() {
	// Nothing to do here.
}

// TODO Implement this interface:
// raft.BatchingFSM extends the FSM interface to add an ApplyBatch function. This can
// optionally be implemented by clients to enable multiple logs to be applied to
// the FSM in batches. Up to MaxAppendEntries could be sent in a batch.
/*
type BatchingFSM interface {
	// ApplyBatch is invoked once a batch of log entries has been committed and
	// are ready to be applied to the FSM. ApplyBatch will take in an array of
	// log entries. These log entries will be in the order they were committed,
	// will not have gaps, and could be of a few log types. Clients should check
	// the log type prior to attempting to decode the data attached. Presently
	// the LogCommand and LogConfiguration types will be sent.
	//
	// The returned slice must be the same length as the input and each response
	// should correlate to the log at the same index of the input. The returned
	// values will be made available in the ApplyFuture returned by Raft.Apply
	// method if that method was called on the same Raft node as the FSM.
	ApplyBatch([]*Log) []interface{}

	FSM
}
*/
func (fsm *fsm) ApplyBatch(logEntries []*raft.Log) []interface{} {
	return fsm.fsmProvider.ApplyBatch(logEntries)
}

// Lookup performs local lookup on the fsm instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *fsm) Lookup(e interface{}) (interface{}, error) {
	query, ok := e.(Query)
	if !ok {
		return nil, fmt.Errorf("invalid query %#v", e)
	}
	return s.fsmProvider.Lookup(query.Key, query.FSMQuery)
}

// Update updates the object using the specified committed raft entry.
func (s *fsm) Update(e sm.Entry) (sm.Result, error) {
	resp, err := s.fsmProvider.Apply(&raft.Log{
		Index: e.Index,
		Data:  e.Cmd,
	})
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
	state, err := f.fsmProvider.SnapshotAll()
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
	var state map[uint16][]byte
	if err := json.NewDecoder(r).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	if err := f.fsmProvider.RestoreAll(state); err != nil {
		return fmt.Errorf("failed to get restore snapshots for finite state machines: %w", err)
	}
	return nil
}

func (f *fsm) Close() (err error) {
	return
}