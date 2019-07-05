package keyvalstore

import (
	"encoding/json"

	"github.com/epsniff/expodb/pkg/server/state-machines"
)

const SetOp = "set"

type KeyValEvent struct {
	RequestType string
	Key         string
	Value       string
}

func NewKeyValEvent(requestType string, key string, value string) KeyValEvent {
	return KeyValEvent{
		RequestType: requestType,
		Key:         key,
		Value:       value,
	}
}

// Marshal and encode the raft type
func (k KeyValEvent) Marshal() ([]byte, error) {
	res, err := json.Marshal(k)
	if err != nil {
		return nil, err
	}
	return machines.EncodeRaftType(KVFSMKey, res), nil
}
