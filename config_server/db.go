package config_server

import (
	"fmt"
	"sync"

	"github.com/senarukana/fundb/protocol"
	"github.com/senarukana/fundb/util"
)

type db struct {
	sync.RWMutex
	dbs    util.StringSet
	shards map[uint32]*protocol.Shard
}

func newDB() *db {
	return &db{
		dbs:    util.NewStringSet(),
		shards: make(map[uint32]*protocol.Shard),
	}
}

func (self *db) withRLock(f func()) {
	self.RLock()
	defer self.RUnlock()
	f()
}

func (self *db) withLock(f func()) {
	self.Lock()
	defer self.Unlock()
	f()
}

func (self *db) createDB(dbName string) (err error) {
	self.withLock(func() {
		if self.dbs.Exists(dbName) {
			err = fmt.Errorf("db %s already existed", dbName)
		}
		self.dbs.Insert(dbName)
	})
	return err
}

func (self *db) listDB() (dbs []string) {
	self.withRLock(func() {
		dbs = self.dbs.ConvertToStrings()
	})
	return dbs
}

func (self *db) createShard(shard *protocol.Shard) {
	self.withLock(func() {
		self.shards[shard.GetShardId()] = shard
	})
}

func (self *db) listShard() []*protocol.Shard {
	res := make([]*protocol.Shard, 0, len(self.shards))
	self.withRLock(func() {
		for _, shard := range self.shards {
			res = append(res, shard)
		}

	})
	return res
}

func (self *db) deleteShard(shardId uint32) {
	self.withLock(func() {
		delete(self.shards, shardId)
	})
}
