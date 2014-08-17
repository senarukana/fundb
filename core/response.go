package core

import (
	"github.com/senarukana/fundb/protocol"
)

type Response struct {
	Error        string
	RowsAffected uint64
	Results      *protocol.RecordList
}
