package meta

import (
	"fmt"
	"math"
)

type Table struct {
	Name        string
	PrimaryKey  string
	SplitKey    string
	NextShardId int
	Shards      []*Shard
}

func NewTable(name string, primaryKey, splitKey string) *Table {
	shard := NewShard(1, name, math.MinInt64, math.MaxInt64)
	return &Table{
		Name:       name,
		PrimaryKey: primaryKey,
		SplitKey:   splitKey,
		Shards:     []*Shard{shard},
	}
}

func (self *Table) GetShard(id uint32) (*Shard, error) {
	for _, shard := range self.Shards {
		if shard.Id == id {
			return shard, nil
		}
	}
	return nil, fmt.Errorf("CAN'T FIND SHARD %d", id)
}

func (self *Table) String() string {
	return fmt.Sprintf("TABLE: [NAME %s, PK %s, SPK: %s, SHARDS: %v]",
		self.Name, self.PrimaryKey, self.SplitKey, self.Shards)
}

// TODO
func (self *Table) GetShardIdsBetween(start, end int64) (shardIds []uint32) {
	for _, shard := range self.Shards {
		shardIds = append(shardIds, shard.Id)
	}
	return shardIds
}

func (self *Table) GetShardIds(ids []int64) (shardIds []uint32) {
	for _, shard := range self.Shards {
		shardIds = append(shardIds, shard.Id)
	}
	return shardIds
}
