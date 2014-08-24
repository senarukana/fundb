package configd

import (
	"sync"

	"github.com/senarukana/fundb/meta"
	"github.com/senarukana/fundb/protocol"
)

type db struct {
	sync.RWMutex
	*meta.MetaData
	nodes nodesInfo
}

type nodesInfo []*protocol.NodeInfo

func newDB() *db {
	return &db{
		MetaData: meta.NewMeta(""),
	}
}

func (self *db) AddNode(node *protocol.NodeInfo) bool {
	self.Lock()
	defer self.Unlock()

	for _, n := range self.nodes {
		if node.GetId() == n.GetId() {
			return true
		}
	}
	self.nodes = append(self.nodes, node)
	return false
}

func (self *db) RemoveNode(node *protocol.NodeInfo) bool {
	self.Lock()
	defer self.Unlock()

	for i, n := range self.nodes {
		if node.GetId() == n.GetId() {
			self.nodes = append(self.nodes[:i-1], self.nodes[i+1:]...)
			return true
		}
	}
	return false
}
