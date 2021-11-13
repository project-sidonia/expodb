package keyvalstore

import (
	"encoding/json"
	"fmt"

	machines "github.com/epsniff/expodb/pkg/server/state-machines"
)

const SetOp = "set"

type KeyValEvent struct {
	RequestType string
	Table       string
	Column      string
	RowKey      string
	Value       string
}

func NewKeyValEvent(requestType, table, col, rowKey, value string) KeyValEvent {
	return KeyValEvent{
		RequestType: requestType,
		Table:       table,
		Column:      col,
		RowKey:      rowKey,
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

func UnmarshalKeyValEvent(buf []byte) (KeyValEvent, error) {
	var e KeyValEvent
	if err := json.Unmarshal(buf, &e); err != nil {
		fmt.Errorf("Failed unmarshaling KeyValEvent. error:%v", err)
	}
	return e, nil
}
