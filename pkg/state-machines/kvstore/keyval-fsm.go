package kvstore

import (
	"encoding/json"
	"fmt"
	"sync"

	machines "github.com/epsniff/expodb/pkg/state-machines"
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
		stateValue: map[string]map[string]string{},
	}
}

type KeyValStateMachine struct {
	mutex      sync.RWMutex
	logger     *zap.Logger
	stateValue map[string]map[string]string
}

// Apply raft log update
func (kv *KeyValStateMachine) Get(namespace, rowkey string) (string, error) {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	if namespace == "" {
		return "", fmt.Errorf("KeyValStateMachine: no namespace provided ")
	}

	if rowkey == "" {
		return "", fmt.Errorf("KeyValStateMachine: no rowkey provided ")
	}

	ns, ok := kv.stateValue[namespace]
	if !ok {
		return "", ErrKeyNotFound
	}
	row, ok := ns[rowkey]
	if !ok {
		return "", ErrKeyNotFound
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
	case UpdateRowOp:
		kv.mutex.Lock()
		defer kv.mutex.Unlock()

		ns, ok := kv.stateValue[e.namespace]
		if !ok {
			ns = map[string]string{}
			kv.stateValue[e.namespace] = ns
		}
		ns[e.RowKey] = e.Value
		kv.logger.Debug("RequestType:KVP: update row",
			zap.String("namespace", e.namespace),
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
