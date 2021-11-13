package keyvalstore

import (
	"encoding/json"
	"fmt"
	"sync"

	machines "github.com/epsniff/expodb/pkg/server/state-machines"
	"go.uber.org/zap"
)

const KVFSMKey = uint16(10)

var (
	ErrKeyNotFound = fmt.Errorf("Key Not Found")
)

func Register(reg machines.FSMProvider, kvfsm *KeyValStateMachine) error {
	return reg.Add(KVFSMKey, kvfsm)
}

func New(logger *zap.Logger) *KeyValStateMachine {
	return &KeyValStateMachine{
		logger:     logger,
		stateValue: map[string]map[string]map[string]string{},
	}
}

type KeyValStateMachine struct {
	mutex      sync.RWMutex
	logger     *zap.Logger
	stateValue map[string]map[string]map[string]string
}

// Apply raft log update
func (kv *KeyValStateMachine) Get(table, rowkey string) (map[string]string, error) {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	if table == "" {
		return nil, fmt.Errorf("KeyValStateMachine: no table provided ")
	}

	if rowkey == "" {
		return nil, fmt.Errorf("KeyValStateMachine: no rowkey provided ")
	}

	tab, ok := kv.stateValue[table]
	if !ok {
		return nil, ErrKeyNotFound
	}
	row, ok := tab[rowkey]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return row, nil
}

// Apply raft log update
func (kv *KeyValStateMachine) Apply(delta []byte) (interface{}, error) {
	e, err := UnmarshalKeyValEvent(delta)
	if err != nil {
		kv.logger.Error("KeyValStateMachine failed to unmarshal kv event:", zap.Error(err))
		return nil, err
	}
	switch e.RequestType {
	case SetOp:
		kv.mutex.Lock()
		defer kv.mutex.Unlock()
		tab, ok := kv.stateValue[e.Table]
		if !ok {
			tab = map[string]map[string]string{}
			kv.stateValue[e.Table] = tab
		}
		row, ok := tab[e.RowKey]
		if !ok {
			row = map[string]string{}
			tab[e.RowKey] = row
		}
		row[e.Column] = e.Value
		kv.logger.Debug("RequestType:KVP: update row",
			zap.String("table", e.Table),
			zap.String("rowkey", e.RowKey),
			zap.String("column", e.Column),
			zap.String("val", e.Value))
		return nil, nil
	default:
		panic(fmt.Sprintf("Unrecognized key value event type in Raft log entry: %v. This is a bug.", e.RequestType))
	}
}

// Restore from a snapshot
func (kv *KeyValStateMachine) Restore(data []byte) error {
	err := json.Unmarshal(data, kv.stateValue) // TODO protobuf or Avro ?
	if err != nil {
		return fmt.Errorf("KeyValStateMachine restore error: %v", err)
	}
	return nil
}

// Save state as bytes for snapshot
func (kv *KeyValStateMachine) Persist() ([]byte, error) {
	data, err := json.Marshal(kv.stateValue)
	if err != nil {
		return nil, fmt.Errorf("KeyValStateMachine persist error: %v", err)
	}
	return data, nil
}
