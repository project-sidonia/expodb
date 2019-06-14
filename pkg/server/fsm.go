package server

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type fsm struct {
	mutex      sync.Mutex
	logger     *zap.Logger
	stateValue map[string]int
}

// Apply applies a Raft log entry to the key-value store.
func (fsm *fsm) Apply(logEntry *raft.Log) interface{} {
	buf := logEntry.Data[:RaftTypeSize]
	msgType := byte(buf[0])

	switch msgType {
	case KeyValType:
		e, err := UnMarshalKeyValEvent(buf)
		if err != nil {
			fsm.logger.Error("Failed to unmarshal response", zap.Error(err))
			return nil
		}
		switch e.RequestType {
		case "set":
			fsm.mutex.Lock()
			defer fsm.mutex.Unlock()
			fsm.stateValue[e.Key] = e.Value
			fsm.logger.Debug("Saving key", zap.Int(e.Key, e.Value))
			return nil
		default:
			panic(fmt.Sprintf("Unrecognized key value event type in Raft log entry: %v. This is a bug.", e.RequestType))
		}
	default:
		panic(fmt.Sprintf("Unrecognized raft message type in Raft log entry: `%v`. This is a bug.", msgType))
	}
}

func (fsm *fsm) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mutex.Lock()
	defer fsm.mutex.Unlock()

	return &fsmSnapshot{stateValue: fsm.stateValue}, nil
}

// Restore stores the key-value store to a previous state.
func (fsm *fsm) Restore(serialized io.ReadCloser) error {
	var snapshot fsmSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}

	fsm.stateValue = snapshot.stateValue
	return nil
}
