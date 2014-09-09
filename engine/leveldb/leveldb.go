package leveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"

	abstract "github.com/senarukana/fundb/engine/interface"
	"github.com/senarukana/fundb/parser"
	"github.com/senarukana/fundb/protocol"

	"code.google.com/p/goprotobuf/proto"
	"github.com/golang/glog"
	"github.com/jmhodges/levigo"
)

const (
	LEVELDB_CACHE_SIZE        = 1024 * 1024 * 16 // 16MB
	LEVELDB_BLOCK_SIZE        = 256 * 1024
	LEVELDB_BLOOM_FILTER_BITS = 64
	LEVELDB_MAX_RECORD_NUM    = 100000
	LEVELDB_MAX_FETCH_SIZE    = 1024 * 1024 // 1MB
	LEVELDB_META_NUM          = 4
	SEPERATOR                 = '|'
	SEED                      = 987654
	RESERVED_ID_COLUMN        = "_id"
)

var (
	EMPTYBYTE             = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	LEVELDB_META_PREFIX   = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	LEVELDB_FIELDS_PREFIX = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10}
)

type LevelDBEngine struct {
	*levigo.DB
	meta *meta
}

func NewLevelDBEngine() abstract.StoreEngine {
	leveldbEngine := &LevelDBEngine{}
	return leveldbEngine
}

// func (self *LevelDBEngine) initMetaInfo() error {
// 	self.schema = newSchema()

// 	ro := levigo.NewReadOptions()
// 	it := self.NewIterator(ro)
// 	defer ro.Close()
// 	defer it.Close()
// 	isValid := true
// 	it.Seek(LEVELDB_META_PREFIX)

// 	for isValid {
// 		isValid = false
// 		if it.Valid() {
// 			key := it.Key()
// 			prefix := key[:8]
// 			if bytes.Compare(prefix, LEVELDB_META_PREFIX) == 0 {
// 				table := &protocol.Table{}
// 				if err := proto.Unmarshal(it.Value(), table); err != nil {
// 					return err
// 				}
// 				ti := newTableInfo(table)

// 				for _, column := range table.GetColumns() {
// 					ti.InsertField(column)
// 				}
// 				self.schema.Insert(table.GetName(), ti)
// 				isValid = true

// 				glog.V(2).Infoln(table)
// 				it.Next()
// 			}
// 		}
// 	}
// 	return nil
// }

func (self *LevelDBEngine) Init(dataPath string, tableName string) error {
	var err error
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(LEVELDB_CACHE_SIZE))
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(LEVELDB_BLOCK_SIZE)
	filter := levigo.NewBloomFilter(LEVELDB_BLOOM_FILTER_BITS)
	opts.SetFilterPolicy(filter)
	self.DB, err = levigo.Open(dataPath, opts)
	if err != nil {
		return err
	}

	meta, err := newMeta(tableName, self)
	if err != nil {
		return err
	}
	self.meta = meta
	return nil
}

// func (self *LevelDBEngine) updateMeta(table string) error {
// 	ti := self.schema.GetTableInfo(table)
// 	wo := levigo.NewWriteOptions()
// 	wo.SetSync(true)
// 	key := append(LEVELDB_META_PREFIX, genereateMetaTableKey(table)...)
// 	val, err := proto.Marshal(ti.Table)
// 	if err != nil {
// 		return err
// 	}
// 	return self.Put(wo, key, val)
// }

// func (self *LevelDBEngine) CreateTable(table string, idtype parser.TableIdType) error {
// 	if self.schema.GetTableInfo(table) != nil {
// 		return fmt.Errorf("Table %s already existed", table)
// 	}

// 	var (
// 		records, size int64
// 		nextid        int64 = 1
// 		fields              = []string{RESERVED_ID_COLUMN}
// 	)
// 	pt := &protocol.Table{
// 		Name:    &table,
// 		Records: &records,
// 		Size:    &size,
// 		Columns: fields,
// 	}
// 	if idtype == parser.TABLE_ID_RANDOM {
// 		pt.Idtype = ID_RANDOM
// 	} else {
// 		pt.Idtype = ID_INCREMENT
// 		pt.Nextid = &nextid
// 	}
// 	ti := newTableInfo(pt)
// 	ti.InsertField(RESERVED_ID_COLUMN)

// 	if err := ti.SyncToDB(self); err != nil {
// 		return err
// 	}

// 	self.schema.Insert(table, ti)
// 	glog.V(2).Infof("Create Table %s complete", table)
// 	return nil
// }

func (self *LevelDBEngine) insertOrDelete(recordList *protocol.RecordList, isDelete bool, ids []int64) error {
	var id int64

	size := 0
	wo := levigo.NewWriteOptions()
	wb := levigo.NewWriteBatch()
	defer wo.Close()
	defer wb.Close()

	for i, record := range recordList.Values {
		if isDelete {
			id = ids[i]
		}
		for fieldIndex, field := range recordList.Fields {
			if field == RESERVED_ID_COLUMN && !isDelete {
				record.Values[fieldIndex].IntVal = &id
			}
			columnId := ti.GetFieldValAndUpdate(field)
			idBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			tsBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			sequenceNumberBuffer := bytes.NewBuffer(make([]byte, 0, 4))
			binary.Write(idBuffer, binary.BigEndian, id)
			binary.Write(tsBuffer, binary.BigEndian, record.GetTimestamp())
			binary.Write(sequenceNumberBuffer, binary.BigEndian, record.GetSequenceNum())
			recordKey := append(append(columnId, idBuffer.Bytes()...), append(tsBuffer.Bytes(), sequenceNumberBuffer.Bytes()...)...)

			if !isDelete {
				glog.V(2).Infof("Insert : %s, recordKey: %v", record.Values[fieldIndex].String(), recordKey)
				data, err := proto.Marshal(record.Values[fieldIndex])
				if err != nil {
					return err
				}
				wb.Put(recordKey, data)
				size += len(data) + len(recordKey)
			} else {
				glog.V(2).Infof("Delete, recordKey : %v", recordKey)
				wb.Delete(recordKey)
			}
		}
	}
	if err := self.Write(wo, wb); err != nil {
		return err
	}
	if !isDelete {
		self.meta.size += size
		self.meta.records += len(recordList.Values)
	} else {
		self.meta.size -= size
		self.meta.records -= len(recordList.Values)
	}
	if err := self.meta.Sync(self); err != nil {
		return err
	}
	return nil
}

func (self *LevelDBEngine) deleteObsoleteRecord(iterators []*levigo.Iterator, recordId []byte) error {
	wo := levigo.NewWriteOptions()
	wb := levigo.NewWriteBatch()

	defer wo.Close()
	defer wb.Close()
	// move all iterator with earliestId to the latest timestamp
	for _, it := range iterators {
		var prevKey []byte
		advanced := false
		for it.Valid() {
			key := it.Key()
			if bytes.Compare(recordId, getIdFromKey(key)) != 0 {
				break
			}
			if prevKey != nil {
				glog.V(2).Infof("Delete obsolete record: key %v", prevKey)
				wb.Delete(prevKey)
			}
			prevKey = key
			it.Next()
			advanced = true
		}
		if advanced {
			if it.Valid() {
				it.Prev()
			} else {
				it.SeekToLast()
			}
		}
	}
	return self.Write(wo, wb)
}

func (self *LevelDBEngine) fetch(condition *parser.WhereExpression, tableName string, fetchFields []string, idStart, idEnd int64, limit int) ([]*protocol.Record, error) {
	if !self.schema.Exist(tableName) {
		return nil, fmt.Errorf("Table %s not existed", tableName)
	}

	idStartBytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	idEndBytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(idStartBytesBuffer, binary.BigEndian, idStart)
	binary.Write(idEndBytesBuffer, binary.BigEndian, idEnd)
	idStartBytes := idStartBytesBuffer.Bytes()
	idEndBytes := idEndBytesBuffer.Bytes()

	fieldPairs, err := self.meta.GetFieldPairs(fetchFields)
	if err != nil {
		return nil, err
	}
	fieldCount := len(fieldPairs)
	iterators := make([]*levigo.Iterator, fieldCount, fieldCount)

	// start the iterators to go through the series data
	for i, fieldPair := range fieldPairs {
		ro := levigo.NewReadOptions()
		defer ro.Close()
		iterators[i] = self.NewIterator(ro)
		iterators[i].Seek(append(fieldPair.Id, idStartBytes...))
		defer iterators[i].Close()
	}

	var records []*protocol.Record
	rawRecordValues := make([]*rawRecordValue, fieldCount, fieldCount)
	isValid := true

	if limit == -1 {
		limit = LEVELDB_MAX_RECORD_NUM
	}

	resultByteCount := 0

	for isValid {
		isValid = false
		var earliestId []byte
		record := &protocol.Record{Values: make([]*protocol.FieldValue, fieldCount, fieldCount)}
		for i, it := range iterators {
			if rawRecordValues[i] == nil && it.Valid() {
				recordKey := newRecordKey(it.Key())
				if len(recordKey.key) >= 28 {
					// check id is between idStart and idEnd
					glog.V(2).Infof("fieldId: %v, key %v, start %v, end %v", fieldPairs[i].Id, it.Key(), idStartBytes, idEndBytes)
					if bytes.Equal(recordKey.getFieldId(), fieldPairs[i].Id) &&
						bytes.Compare(recordKey.getId(), idStartBytes) > -1 && bytes.Compare(recordKey.getId(), idEndBytes) < 1 {
						v := it.Value()
						rawRecordValues[i] = &rawRecordValue{recordKey: recordKey, value: v}
						idCompare := bytes.Compare(recordKey.getId(), earliestId)

						// find the earliest id
						if earliestId == nil || idCompare < 0 {
							earliestId = recordKey.getId()
						}
					}
				}
			}
		}
		// move all iterator with earliestId to the newest record and delete the obsolete record
		err := self.deleteObsoleteRecord(iterators, earliestId)
		if err != nil {
			return nil, err
		}

		for i, it := range iterators {
			if it.Valid() && rawRecordValues[i] != nil && bytes.Equal(rawRecordValues[i].getId(), earliestId) {
				var id, ts int64
				var sequence uint32
				isValid = true
				it.Next()
				fv := &protocol.FieldValue{}
				err := proto.Unmarshal(rawRecordValues[i].value, fv)
				if err != nil {
					return nil, err
				}
				resultByteCount += len(rawRecordValues[i].value)
				binary.Read(bytes.NewBuffer(rawRecordValues[i].getId()), binary.BigEndian, &id)
				binary.Read(bytes.NewBuffer(rawRecordValues[i].getTimestamp()), binary.BigEndian, &ts)
				binary.Read(bytes.NewBuffer(rawRecordValues[i].getSequenceNum()), binary.BigEndian, &sequence)

				record.Values[i] = fv
				record.Id = &id
				record.Timestamp = &ts
				record.SequenceNum = &sequence
				rawRecordValues[i] = nil
			}
		}
		if isValid {
			limit--
			records = append(records, record)

			// add byte count for the timestamp and the sequence
			resultByteCount += 16

			// check if we should send the batch along
			if resultByteCount > LEVELDB_MAX_FETCH_SIZE {
				break
			}
		}
		if limit < 1 {
			break
		}
	}

	glog.Errorf("filtered results = %d", len(records))
	return filterCondition(records, condition, fetchFields)
}

func (self *LevelDBEngine) Insert(recordList *protocol.RecordList) error {
	return self.insertOrDelete(recordList, false, nil)
}

func (self *LevelDBEngine) Delete(query *parser.DeleteQuery) (int64, error) {
	condition, idStart, idEnd, err := parser.GetIdCondition(query.WhereExpression)
	if err != nil {
		return -1, err
	}

	// TODO: Make it in parser
	if query.WhereExpression == nil {
		return -1, fmt.Errorf("NO WHERE EXPRESSION IN DELETE")
	}

	fields := query.WhereExpression.GetConditionFields()

	glog.V(1).Infof("table %s, fields %v, start %d, end %d", query.Table, fields, idStart, idEnd)
	records, err := self.fetch(condition, query.Table, fields, idStart, idEnd, -1)
	if err != nil {
		return -1, err
	}

	ids := getIdsFromRecords(fields, records)

	// delete all fields
	deleteFields := ti.GetAllFields()
	recordList := &protocol.RecordList{
		Name:   &query.Table,
		Fields: deleteFields,
		Values: records,
	}
	if err := self.insertOrDelete(recordList, true, ids); err != nil {
		return -1, err
	} else {
		return int64(len(recordList.Values)), nil
	}
}

func (self *LevelDBEngine) getSelectAndFetchFields(query *parser.SelectQuery) ([]string, []string) {
	if !query.IsStar {
		return query.GetSelectFields(), query.GetSelectAndConditionFields()
	}
	ti := self.schema.GetTableInfo(query.Table)
	allFields := ti.GetAllFields()
	return allFields, allFields
}

func (self *LevelDBEngine) Fetch(query *parser.SelectQuery) (*protocol.RecordList, error) {
	condition, idStart, idEnd, err := parser.GetIdCondition(query.WhereExpression)
	selectFields, fetchFields := self.getSelectAndFetchFields(query)

	glog.V(1).Infof("table %s, selectFields %v, fetchFields %v, start %d, end %d, limit %d", query.Table, selectFields, fetchFields, idStart, idEnd, query.Limit)

	records, err := self.fetch(condition, query.Table, fetchFields, idStart, idEnd, query.Limit)
	if err != nil {
		return nil, err
	}

	filteredResult := filterFields(records, selectFields, fetchFields)

	res := &protocol.RecordList{
		Name:   &query.Table,
		Fields: selectFields,
		Values: filteredResult,
	}
	return res, nil
}

func (self *LevelDBEngine) Close() error {
	return self.Close()
}

func (self *LevelDBEngine) DropDatabase(database string) error {
	return nil
}
