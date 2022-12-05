package raft

import machines "github.com/epsniff/expodb/pkg/server/state-machines"

func NewMockAgent() *MockAgent {
	return &MockAgent{
		fsmp: machines.FSMProvider{},
	}
}

type MockAgent struct {
	fsmp machines.FSMProvider
}

func (m *MockAgent) AddStateMachine(key string, sm machines.StateMachine) error {
	return nil
}

func (m *MockAgent) AddVoter(id, peerAddress string) error {
	return nil
}

func (m *MockAgent) Apply(key uint16, val RaftEntry) error {
	return nil
}

func (m *MockAgent) IsLeader() bool {
	return true
}

func (m *MockAgent) LeaderAddress() string {
	return ""
}

func (m *MockAgent) Shutdown() error {
	return nil
}
