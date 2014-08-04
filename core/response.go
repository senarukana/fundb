package core

import (
	"github.com/senarukana/fundb/protocol"
)

type ResponseValue struct {
	Value string
	Type  protocol.FieldType
}

type Response struct {
	Error        error
	RowsAffected uint64
	Results      [][]*ResponseValue
}
