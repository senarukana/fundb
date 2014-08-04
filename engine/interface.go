package engine

import (
	"github.com/senarukana/fundb/protocol"
)

type storeEngine interface {
	Init(dataPath string) error
	Insert(database string, recordList *protocol.RecordList) error
	DropDatabase(database string) error
	Close() error
}
