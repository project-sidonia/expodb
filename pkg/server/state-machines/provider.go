package machines

import (
	"encoding/binary"
	"fmt"

	"github.com/hashicorp/raft"
)

type StateMachine interface {
	Apply(delta []byte) (interface{}, error) // Apply raft log update
	Restore(data []byte) error               // Restore a subsection from snapshot
	Persist() ([]byte, error)                // Save state as bytes for snapshot
}

type FSMProvider map[uint16]StateMachine

// Add a new handler to the registry, this most be done in an init style at startup
// because this func doesn't lock the backing map.
func (fsmr FSMProvider) Add(key uint16, sm StateMachine) error {
	if _, ok := fsmr[key]; ok {
		return fmt.Errorf("Mutiple FSM Registry entires for %v", key)
	}

	fsmr[key] = sm

	return nil
}

// looks up the correct handler and calls it, the returned interface is what we'll return to
// Hashicorps raft.  The error is incase we don't have a registered handler or if the
// handler returns an error.
func (fsmr FSMProvider) Apply(logEntry *raft.Log) (interface{}, error) {
	data := logEntry.Data[:len(logEntry.Data)-2] // The last byte should be the typeS
	// fsm.logger.Debug("DEBUG", zap.ByteString("apply.Buf", buf))
	keyB := logEntry.Data[len(logEntry.Data)-2:] // read just the last byte for the type
	// fsm.logger.Debug("DEBUG", zap.ByteString("apply.type", []byte{msgType}))
	key := binary.BigEndian.Uint16(keyB)

	handler, ok := fsmr[key]
	if !ok {
		return nil, fmt.Errorf("No handler found for key: %d", key)
	}
	return handler.Apply(data)
}

func (fsmr FSMProvider) SnapshotAll() (map[uint16][]byte, error) {
	s := map[uint16][]byte{}
	for k, fsm := range fsmr {
		data, err := fsm.Persist()
		if err != nil {
			return nil, fmt.Errorf("Error from fsm.Persist %d (type:%T): error:%v", k, fsm, err)
		}
		s[k] = data
	}
	return s, nil
}

func (fsmr FSMProvider) RestoreAll(vals map[uint16][]byte) error {
	for k, fsm := range fsmr {
		data, ok := vals[k]
		if !ok {
			continue
		}
		err := fsm.Restore(data)
		if err != nil {
			return fmt.Errorf("Error from fsm.Restore %d (type:%T): error:%v", k, fsm, err)
		}
	}
	return nil
}
