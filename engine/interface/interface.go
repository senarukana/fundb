package engine

import (
	"github.com/senarukana/fundb/parser"
	"github.com/senarukana/fundb/protocol"
)

type StoreEngine interface {
	Init(dataPath string) error
	CreateTable(table string, idtype parser.TableIdType) error
	Insert(recordList *protocol.RecordList) error
	Fetch(query *parser.SelectQuery) (*protocol.RecordList, error)
	Delete(query *parser.DeleteQuery) (int64, error)
	Close() error
}
