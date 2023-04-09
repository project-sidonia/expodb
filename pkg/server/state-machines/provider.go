package machines

type RaftEntry interface {
	// By convention the messages self marshal and encode thier fsm type as the last
	// 2 bytes of the bytes.
	Marshal() ([]byte, error)
}

// StateMachine is the interface that all state machines must implement.
type StateMachine interface {
	// Apply raft log update for this state machine
	Apply(delta []byte) (interface{}, error)

	Lookup(query interface{}) (interface{}, error)

	// Restore a subsection from snapshot for this state machine
	Restore(data []byte) error

	// Save state as bytes for snapshot for this state machine
	Persist() ([]byte, error)
}
