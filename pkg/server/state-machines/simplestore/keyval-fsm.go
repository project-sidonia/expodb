package simplestore

import (
	"encoding/json"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

const KVFSMKey = uint16(10)

// Copied from https://github.com/lni/dragonboat-example/blob/master/optimistic-write-lock/fsm.go
const (
	ResultCodeFailure = iota
	ResultCodeSuccess
	ResultCodeVersionMismatch
)

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
)

func New(logger *zap.Logger) *KeyValStateMachine {
	return &KeyValStateMachine{
		logger:     logger,
		stateValue: map[string]map[string]map[string]string{},
	}
}

type KeyValStateMachine struct {
	mutex  sync.RWMutex
	logger *zap.Logger
	// table -> rowkey -> column -> value
	stateValue map[string]map[string]map[string]string
}

type Query struct {
	Table  string
	RowKey string
}

func (kv *KeyValStateMachine) Lookup(e interface{}) (interface{}, error) {
	query, ok := e.(Query)
	if !ok {
		return nil, fmt.Errorf("invalid query %#v", e)
	}
	return kv.Get(query.Table, query.RowKey)
}

func (kv *KeyValStateMachine) Get(table, rowkey string) (map[string]string, error) {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	if table == "" {
		return nil, fmt.Errorf("KeyValStateMachine: no table provided")
	}

	if rowkey == "" {
		return nil, fmt.Errorf("KeyValStateMachine: no rowkey provided")
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
		return ResultCodeFailure, fmt.Errorf("failed to unmarshal kv event: %w", err)
	}
	switch e.RequestType {
	case UpdateRowOp:
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
		return []byte(e.Value), nil
	default:
		panic(fmt.Sprintf("Unrecognized key value event type in Raft log entry: %v. This is a bug.", e.RequestType))
	}
}

// Restore from a snapshot
func (kv *KeyValStateMachine) Restore(data []byte) error {
	err := json.Unmarshal(data, &kv.stateValue) // TODO protobuf or Avro ?
	if err != nil {
		return fmt.Errorf("restore error on KeyValStateMachine: %w", err)
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
