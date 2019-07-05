package keyvalstore

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/epsniff/expodb/pkg/server/state-machines"
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
		stateValue: map[string]string{},
	}
}

type KeyValStateMachine struct {
	mutex      sync.RWMutex
	logger     *zap.Logger
	stateValue map[string]string
}

// Apply raft log update
func (kv *KeyValStateMachine) Get(key string) (string, error) {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()
	val, ok := kv.stateValue[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return val, nil
}

// Apply raft log update
func (kv *KeyValStateMachine) Apply(delta []byte) (interface{}, error) {
	e, err := kv.unmarshalKeyValEvent(delta)
	if err != nil {
		kv.logger.Error("Failed to unmarshal response", zap.Error(err))
		return nil, err
	}
	switch e.RequestType {
	case SetOp:
		kv.mutex.Lock()
		defer kv.mutex.Unlock()
		kv.stateValue[e.Key] = e.Value
		kv.logger.Debug("RequestType:KVP: key/val stored", zap.String(e.Key, e.Value))
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

func (kv *KeyValStateMachine) unmarshalKeyValEvent(buf []byte) (KeyValEvent, error) {
	var e KeyValEvent
	if err := json.Unmarshal(buf, &e); err != nil {
		fmt.Errorf("Failed unmarshaling Raft log entry. error:%v", err)
	}
	return e, nil
}
