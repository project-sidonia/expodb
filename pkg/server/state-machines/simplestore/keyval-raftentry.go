package simplestore

import (
	"encoding/json"
	"fmt"
)

const UpdateRowOp = "update_row"

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
	return res, nil
}

func UnmarshalKeyValEvent(buf []byte) (KeyValEvent, error) {
	var e KeyValEvent
	if err := json.Unmarshal(buf, &e); err != nil {
		return e, fmt.Errorf("failed unmarshaling KeyValEvent: %w", err)
	}
	return e, nil
}
