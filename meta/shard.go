package meta

import (
	"fmt"
)

type Shard struct {
	Id         uint32
	TableName  string
	RangeStart int64
	RangeEnd   int64
	ServerIds  []uint32
	Records    int
	Size       int
}

func NewShard(id uint32, tableName string, rangeStart, rangeEnd int64) *Shard {
	return &Shard{
		Id:         id,
		TableName:  tableName,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
	}
}

func (self *Shard) String() string {
	return fmt.Sprintf("SHARD: [ID %d, START: %d, END: %d, Records: %d, Size: %d]",
		self.Id, self.RangeStart, self.RangeEnd, self.Records, self.Size)
}
