package server

import (
	"fmt"
	"sync"

	"github.com/hashicorp/serf/serf"
)

// metadata contains the state of cluster, and is used to
// provide raft address and http address for nodes in the cluster.
type metadata struct {
	mu sync.RWMutex

	nodesById      map[string]*nodedata
	nodesByRaftAdd map[string]*nodedata
}

func NewMetadata() *metadata {
	return &metadata{
		nodesById:      map[string]*nodedata{},
		nodesByRaftAdd: map[string]*nodedata{},
	}
}

// Add adds a node to the metadata.
func (m *metadata) Add(me serf.Member) (*nodedata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, err := nodeDataFromSerf(me)
	if err != nil {
		return nil, err
	}
	m.nodesById[meta.id] = meta
	m.nodesByRaftAdd[meta.raftAddr] = meta
	return meta, nil
}

// FindByRaftAddr finds a node by its raft address.
func (m *metadata) FindByRaftAddr(n string) (*nodedata, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	meta, ok := m.nodesByRaftAdd[n]
	return meta, ok
}

type nodedata struct {
	id       string
	raftAddr string
	httpAddr string
}

// nodeDataFromSerf returns a nodedata from a serf member.
func nodeDataFromSerf(m serf.Member) (*nodedata, error) {
	id, ok := m.Tags["id"] // TODO replace magic strings here and in serf-agent setup with consts..
	if !ok {
		return nil, fmt.Errorf("metadata: missing `id` tag?")
	}

	peerAddr, ok := m.Tags["raft_addr"]
	if !ok {
		return nil, fmt.Errorf("metadata: missing `raft_addr` tag?")
	}
	peerPort, ok := m.Tags["raft_port"]
	if !ok {
		return nil, fmt.Errorf("metadata: missing `raft_port` tag?")
	}
	raftAddress := fmt.Sprintf("%s:%s", peerAddr, peerPort)

	peerAddr, ok = m.Tags["http_addr"]
	if !ok {
		return nil, fmt.Errorf("metadata: missing `http_addr` tag?")
	}
	peerPort, ok = m.Tags["http_port"]
	if !ok {
		return nil, fmt.Errorf("metadata: missing `http_port` tag?")
	}
	httpAddress := fmt.Sprintf("%s:%s", peerAddr, peerPort)

	return &nodedata{
		id:       id,
		raftAddr: raftAddress,
		httpAddr: httpAddress,
	}, nil
}

// ID returns the ID of the current node.
func (n *nodedata) Id() string {
	return n.id
}

// RafAddr returns the raft address of the current node.
func (n *nodedata) RaftAddr() string {
	return n.raftAddr
}

// HttpAddr returns the http address of the current node.
func (n *nodedata) HttpAddr() string {
	return n.httpAddr
}
