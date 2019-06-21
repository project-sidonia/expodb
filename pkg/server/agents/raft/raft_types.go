package raft

import (
	"encoding/json"
	"fmt"
)

// FooterRaftType marks the last byte in the buffer with it's type info.
func FooterRaftType(rtype byte, buf []byte) []byte {
	buf = append(buf, rtype)
	return buf
}

const (
	KeyValType byte = '0'
)

type KeyValEvent struct {
	RequestType string
	Key         string
	Value       int //TODO support more than ints
}

func NewKeyValEvent(requestType string, key string, value int) KeyValEvent {
	return KeyValEvent{
		RequestType: requestType,
		Key:         key,
		Value:       value,
	}
}

func (k KeyValEvent) Marshal() ([]byte, error) {
	res, err := json.Marshal(k)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func UnMarshalKeyValEvent(buf []byte) (KeyValEvent, error) {
	var e KeyValEvent
	if err := json.Unmarshal(buf, &e); err != nil {
		fmt.Errorf("Failed unmarshaling Raft log entry. error:%v", err)
	}
	return e, nil
}
