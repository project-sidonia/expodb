package usecases

import (
	"fmt"

	"github.com/epsniff/expodb/pkg/state-machines/kvstore"
)

type KeyValueStore struct {
	leaderManager interfaces.LeaderManager
}

func (k *KeyValueStore) StoreSingleValue(namespace string, key string, value string) error {

	if !k.leaderManager.IsLeader() {
		// TODO: forward to leader
		return fmt.Errorf("TODO DEAL WITH not leader")
	}
	// if Not Leader make http request to leader to ask them to do the Set Command.

	kve := kvstore.NewKeyValEvent(kvstore.UpdateRowOp, table, col, key, val)
	return n.raftAgent.Apply(kvstore.KVFSMKey, kve)
}
func (k *KeyValueStore) RetrieveSingleValue(namespace string, key string) (string, error) {
	panic("implement me")
}
