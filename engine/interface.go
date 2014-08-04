package engine

import (
	"github.com/senarukana/fundb/protocol"
)

type storeEngine interface {
	Init(dataPath string) error
	Insert(recordList *protocol.RecordList) error
	Close() error
}
