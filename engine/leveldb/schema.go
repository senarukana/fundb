package leveldb

import (
	"fmt"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/senarukana/fundb/protocol"

	"code.google.com/p/goprotobuf/proto"
)

type FieldPair struct {
	Name string
	Id   []byte
}

type tableInfo struct {
	sync.RWMutex
	fields map[string][]byte
	*protocol.Table
}

func newTableInfo(table *protocol.Table) *tableInfo {
	return &tableInfo{
		fields: make(map[string][]byte),
		Table:  table,
	}
}

func (self *tableInfo) InsertField(field string) {
	self.fields[field] = genereateColumnId(self.Table.GetName(), field)
}

func (self *tableInfo) GetFieldValAndUpdate(field string) []byte {
	if val, ok := self.fields[field]; ok {
		return val
	} else {
		self.InsertField(field)
		return self.fields[field]
	}
}

func (self *tableInfo) GetAllFields() []string {
	res := make([]string, 0, len(self.fields))
	for field, _ := range self.fields {
		res = append(res, field)
	}
	return res
}

func (self *tableInfo) GetAllFieldPairs() []*FieldPair {
	res := make([]*FieldPair, 0, len(self.fields))
	for field, fieldId := range self.fields {
		fieldPair := &FieldPair{
			Name: field,
			Id:   fieldId,
		}
		res = append(res, fieldPair)
	}
	return res
}

func (self *tableInfo) GetFieldPairs(fields []string) ([]*FieldPair, error) {
	res := make([]*FieldPair, 0, len(self.fields))
	for _, field := range fields {
		fieldId, ok := self.fields[field]
		if !ok {
			return nil, fmt.Errorf("Unknown field %s", field)
		}
		fieldPair := &FieldPair{
			Name: field,
			Id:   fieldId,
		}
		res = append(res, fieldPair)
	}
	return res, nil
}

func (self *tableInfo) SyncToDB(db *LevelDBEngine) error {
	tableKey := append(LEVELDB_META_PREFIX, []byte(self.Table.GetName())...)
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	val, err := proto.Marshal(self.Table)
	if err == nil {
		if err = db.Put(wo, tableKey, val); err != nil {
			return err
		}
	}
	return nil
}

type schema struct {
	sync.RWMutex
	tables map[string]*tableInfo
}

func newSchema() *schema {
	return &schema{
		tables: make(map[string]*tableInfo),
	}
}

func (self *schema) Exist(tableName string) bool {
	if _, ok := self.tables[tableName]; ok {
		return true
	} else {
		return false
	}
}

func (self *schema) GetTableInfo(tableName string) *tableInfo {
	if ti, ok := self.tables[tableName]; ok {
		return ti
	} else {
		return nil
	}
}

func (self *schema) Insert(tableName string, ti *tableInfo) {
	if self.Exist(tableName) {
		panic(fmt.Sprintf("%s already existed", tableName))
	}
	self.tables[tableName] = ti
}
