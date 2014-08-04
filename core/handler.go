package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/senarukana/fundb/engine"
	"github.com/senarukana/fundb/parser"
	"github.com/senarukana/fundb/protocol"
)

const (
	defaultDatabase = "default"
)

type EngineHandler struct {
	storeEngine           *engine.EngineManager
	currentSequenceNumber uint32
	sequenceNumberLock    sync.Mutex
}

func NewEngineHandler(engineName, dataPath string) (*EngineHandler, error) {
	engineManager, err := engine.NewEngineManager(engineName, dataPath)
	if err != nil {
		return nil, err
	}
	return &EngineHandler{
		storeEngine: engineManager,
	}, nil
}

func (self *EngineHandler) Query(sql string) *Response {
	query, err := parser.ParseQuery(sql)
	if err != nil {
		return &Response{
			Error: err,
		}
	}
	switch query.Type {
	case parser.QUERY_INSERT:
		return self.insert(query.Query.(*parser.InsertQuery))
	}
	return nil
}

func (self *EngineHandler) validInsertQuery(query *parser.InsertQuery) error {
	if len(query.ValueList.Values[0].Items) != len(query.Fields.Fields) {
		return fmt.Errorf("syntax error: Incompatible fields(%d) and values(%d)",
			len(query.ValueList.Values[0].Items), len(query.Fields.Fields))
	}
	var paramCount = -1
	for valueIndex, valueItems := range query.ValueList.Values {
		if paramCount == -1 {
			paramCount = len(valueItems.Items)
		}
		if paramCount != len(valueItems.Items) {
			return fmt.Errorf("syntax error: Incompatible value paramters in %d, paremter num is %d, exptected %d",
				valueIndex, len(valueItems.Items), paramCount)
		}
	}
	return nil
}

func (self *EngineHandler) insert(query *parser.InsertQuery) *Response {
	recordList := &protocol.RecordList{
		Name:   &query.Table,
		Fields: query.Fields.Fields,
		Values: make([]*protocol.Record, 0, len(query.ValueList.Values)),
	}
	now := time.Now().UnixNano()
	self.sequenceNumberLock.Lock()
	defer self.sequenceNumberLock.Unlock()
	for _, valueItems := range query.ValueList.Values {
		sn := self.currentSequenceNumber
		record := &protocol.Record{
			Timestamp:   &now,
			SequenceNum: &sn,
			Values:      valueItems.Items,
		}
		self.currentSequenceNumber++
		recordList.Values = append(recordList.Values, record)
	}
	err := self.storeEngine.Insert(defaultDatabase, recordList)
	if err != nil {
		return &Response{
			Error: err,
		}
	} else {
		return &Response{
			RowsAffected: uint64(len(query.ValueList.Values)),
		}
	}
}
