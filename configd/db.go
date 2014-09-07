package configd

import (
	"sync"
	"time"

	"github.com/senarukana/fundb/meta"
	"github.com/senarukana/fundb/protocol"
)

type db struct {
	sync.RWMutex
	*meta.MetaData
	nodes nodesInfo
}

type node struct {
	*protocol.NodeInfo
	lastUpdate time.Time
}

func newNode(ni *protocol.NodeInfo) *node {
	return &node{
		NodeInfo:   ni,
		lastUpdate: time.Now(),
	}
}

type nodesInfo []*node

func (self nodesInfo) filterByActive(inactiveTimeout time.Duration) nodesInfo {
	var res nodesInfo
	now := time.Now()
	for _, ni := range self {
		if now.Sub(ni.lastUpdate) > inactiveTimeout {
			continue
		}
		res = append(res, ni)
	}
	return res
}

func newDB() *db {
	return &db{
		MetaData: meta.NewMeta(),
	}
}

func (self *db) AddNode(node *node) bool {
	self.Lock()
	defer self.Unlock()

	self.nodes = append(self.nodes, node)
	return false
}

func (self *db) RemoveNode(ni *node) bool {
	self.Lock()
	defer self.Unlock()

	for i, n := range self.nodes {
		if ni.GetId() == n.GetId() {
			if i == 0 {
				self.nodes = self.nodes[i+1:]
			} else if i == len(self.nodes)-1 {
				self.nodes = self.nodes[:i]
			} else {
				self.nodes = append(self.nodes[:i-1], self.nodes[i+1:]...)
			}
			return true
		}
	}
	return false
}
