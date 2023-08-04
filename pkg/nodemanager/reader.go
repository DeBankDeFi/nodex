package nodemanager

import (
	"sync"

	"github.com/DeBankDeFi/nodex/pkg/pb"
)

// ReaderNodePool is a pool of nodes.
type ReaderNodePool struct {
	nodes map[string]*pb.NodeId
	lock  sync.RWMutex
}

func NewReaderNodePool() *ReaderNodePool {
	return &ReaderNodePool{
		nodes: make(map[string]*pb.NodeId),
	}
}

func (p *ReaderNodePool) Update(nodeId *pb.NodeId) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.nodes[nodeId.Uuid] = nodeId
}

// OffLine marks a node as offline.
func (p *ReaderNodePool) OffLine(uuid string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.nodes, uuid)
}

func (p *ReaderNodePool) GetReaders() []*pb.NodeId {
	p.lock.RLock()
	defer p.lock.RUnlock()
	var readers []*pb.NodeId
	for _, node := range p.nodes {
		if node.Role == pb.NodeRole_READER {
			readers = append(readers, node)
		}
	}
	return readers
}
