package engine

import (
	"github.com/senarukana/fundb/parser"
	"github.com/senarukana/fundb/protocol"
)

type storeEngine interface {
	Init(dataPath string) error
	CreateTable(table string, idtype parser.TableIdType) error
	Insert(recordList *protocol.RecordList) error
	Fetch(table string, columns []string,
		idStart, idEnd int64, whereExpr *parser.WhereExpression, limit int) (*protocol.RecordList, error)
	Close() error
}
