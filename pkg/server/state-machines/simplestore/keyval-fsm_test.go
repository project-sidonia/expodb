package simplestore

import (
	"reflect"
	"sync"
	"testing"

	"go.uber.org/zap"
)

func TestKeyValStateMachine_Get(t *testing.T) {
	type fields struct {
		logger     *zap.Logger
		stateValue map[string]map[string]map[string]string
	}
	type args struct {
		table  string
		rowkey string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			name: "TestKeyValStateMachine_Get_1",
			fields: fields{
				logger: zap.NewNop(),
				stateValue: map[string]map[string]map[string]string{
					"table1": {
						"rowkey1": {
							"col1": "val1",
						},
					},
				},
			},
			args: args{
				table:  "table1",
				rowkey: "rowkey1",
			},
			want: map[string]string{
				"col1": "val1",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := &KeyValStateMachine{
				mutex:      sync.RWMutex{},
				logger:     tt.fields.logger,
				stateValue: tt.fields.stateValue,
			}
			got, err := kv.Get(tt.args.table, tt.args.rowkey)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValStateMachine.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KeyValStateMachine.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKeyValStateMachine_Apply(t *testing.T) {
	type fields struct {
		logger     *zap.Logger
		stateValue map[string]map[string]map[string]string
	}
	type args struct {
		delta []byte
	}

	val := NewKeyValEvent(UpdateRowOp, "table1", "rowkey1", "col1", "val1")
	valBytes, err := val.Marshal()
	if err != nil {
		t.Errorf("KeyValStateMachine.Apply() error = %v", err)
		return
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "TestKeyValStateMachine_Apply_1",
			fields: fields{
				logger: zap.NewNop(),
				stateValue: map[string]map[string]map[string]string{
					"table1": {
						"rowkey1": {
							"col1": "val1",
						},
					},
				},
			},
			args: args{
				delta: valBytes,
			},
			want:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := &KeyValStateMachine{
				mutex:      sync.RWMutex{},
				logger:     tt.fields.logger,
				stateValue: tt.fields.stateValue,
			}
			got, err := kv.Apply(tt.args.delta)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValStateMachine.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KeyValStateMachine.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}
