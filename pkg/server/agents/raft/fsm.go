package raft

import (
	"encoding/json"
	"io"

	machines "github.com/epsniff/expodb/pkg/server/state-machines"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type fsm struct {
	logger *zap.Logger

	fsmProvider machines.FSMProvider
}

// Apply applies a Raft log entry to the key-value store.
func (fsm *fsm) Apply(logEntry *raft.Log) interface{} {
	res, err := fsm.fsmProvider.Apply(logEntry)
	if err != nil {
		fsm.logger.Error("Failed to apply raft log to finite state machines: err:%v", zap.Error(err))
		return err
	}
	return res
}

func (fsm *fsm) Snapshot() (raft.FSMSnapshot, error) {
	state, err := fsm.fsmProvider.SnapshotAll()
	if err != nil {
		fsm.logger.Error("Failed to get snapshots from finite state machines: err:%v", zap.Error(err))
		return nil, err
	}
	return &fsmSnapshot{state: state}, nil
}

// Restore stores the key-value store to a previous state.
func (fsm *fsm) Restore(serialized io.ReadCloser) error {
	var snapshot fsmSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}
	err := fsm.fsmProvider.RestoreAll(snapshot.state)
	if err != nil {
		fsm.logger.Error("Failed to get restore snapshots for finite state machines: err:%v", zap.Error(err))
		return err
	}
	return nil
}

type fsmSnapshot struct {
	state map[uint16][]byte `json:"snapshot_state"`
}

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

func (f *fsmSnapshot) Release() {}
