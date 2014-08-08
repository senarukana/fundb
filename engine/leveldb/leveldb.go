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
	EMPTYBYTE                   = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	LEVELDB_META_PREFIX         = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	LEVELDB_TABLE_FIELDS_PREFIX = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10}

	ID_INCREMENT = []byte{0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ID_RANDOM    = []byte{0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
)

type rawRecordValue struct {
	id       []byte
	sequence []byte
	value    []byte
}

type LevelDBEngine struct {
	*levigo.DB
	schema *schema
}

func NewLevelDBEngine() abstract.StoreEngine {
	leveldbEngine := new(LevelDBEngine)
	return leveldbEngine
}

func (self *LevelDBEngine) initMetaInfo() error {
	self.schema = newSchema()

	ro := levigo.NewReadOptions()
	it := self.NewIterator(ro)
	defer ro.Close()
	defer it.Close()
	isValid := true
	it.Seek(LEVELDB_META_PREFIX)

	for isValid {
		isValid = false
		if it.Valid() {
			key := it.Key()
			prefix := key[:8]
			if bytes.Compare(prefix, LEVELDB_META_PREFIX) == 0 {
				table := &protocol.Table{}
				if err := proto.Unmarshal(it.Value(), table); err != nil {
					return err
				}
				ti := newTableInfo(table)

				for _, column := range table.GetColumns() {
					ti.InsertField(column)
				}
				self.schema.Insert(table.GetName(), ti)
				isValid = true

				glog.V(2).Infoln(table)
				it.Next()
			}
		}
	}
	return nil
}

func (self *LevelDBEngine) Init(dataPath string) (err error) {
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

	return self.initMetaInfo()
}

func (self *LevelDBEngine) updateMeta(table string) error {
	ti := self.schema.GetTableInfo(table)
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	key := append(LEVELDB_META_PREFIX, genereateMetaTableKey(table)...)
	val, err := proto.Marshal(ti.Table)
	if err != nil {
		return err
	}
	return self.Put(wo, key, val)
}

func (self *LevelDBEngine) CreateTable(table string, idtype parser.TableIdType) error {
	if self.schema.GetTableInfo(table) != nil {
		return fmt.Errorf("Table %s already existed", table)
	}

	var (
		records, size int64
		nextid        int64 = 1
		fields              = []string{RESERVED_ID_COLUMN}
	)
	pt := &protocol.Table{
		Name:    &table,
		Records: &records,
		Size:    &size,
		Columns: fields,
	}
	if idtype == parser.TABLE_ID_RANDOM {
		pt.Idtype = ID_RANDOM
	} else {
		pt.Idtype = ID_INCREMENT
		pt.Nextid = &nextid
	}
	ti := newTableInfo(pt)
	ti.InsertField(RESERVED_ID_COLUMN)

	if err := ti.SyncToDB(self); err != nil {
		return err
	}

	self.schema.Insert(table, ti)
	glog.V(2).Infof("Create Table %s complete", table)
	return nil
}

func (self *LevelDBEngine) insertOrDelete(recordList *protocol.RecordList, isDelete bool, ids []int64) error {
	var id int64

	size := 0
	wo := levigo.NewWriteOptions()
	wb := levigo.NewWriteBatch()
	defer wo.Close()
	defer wb.Close()

	ti := self.schema.GetTableInfo(recordList.GetName())
	if ti == nil {
		return fmt.Errorf("Table %s not existed", recordList.GetName())
	}
	if bytes.Equal(ti.GetIdtype(), ID_INCREMENT) {
		ti.Lock()
		defer ti.Unlock()
	}
	for i, record := range recordList.Values {
		if isDelete {
			id = ids[i]
			fmt.Println(id)
		} else {
			if bytes.Equal(ti.GetIdtype(), ID_RANDOM) {
				id = rand.Int63n(SEED)
			} else if bytes.Equal(ti.GetIdtype(), ID_INCREMENT) {
				id = ti.GetNextid()
			} else {
				panic("INVALID Id TYPE")
			}
		}
		for fieldIndex, field := range recordList.Fields {
			if field == RESERVED_ID_COLUMN && !isDelete {
				record.Values[fieldIndex].IntVal = &id
			}
			columnId := ti.GetFieldValAndUpdateNoLock(field)
			idBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			sequenceNumberBuffer := bytes.NewBuffer(make([]byte, 0, 4))
			binary.Write(idBuffer, binary.BigEndian, id)
			binary.Write(sequenceNumberBuffer, binary.BigEndian, record.GetSequenceNum())
			recordKey := append(append(columnId, idBuffer.Bytes()...), sequenceNumberBuffer.Bytes()...)

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
		if !isDelete {
			(*ti.Nextid)++
		}
	}
	if err := self.Write(wo, wb); err != nil {
		return err
	}
	if err := ti.SyncToDB(self); err != nil {
		return err
	}
	if !isDelete {
		*ti.Size += int64(size)
		*ti.Records += int64(len(recordList.Values))
	} else {
		*ti.Size -= int64(size)
		*ti.Records -= int64(len(recordList.Values))
	}
	return self.updateMeta(recordList.GetName())
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

	ti := self.schema.GetTableInfo(tableName)
	fieldPairs, err := ti.GetFieldPairs(fetchFields)
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
		latestIdRaw := make([]byte, 8, 8)
		latestSequenceRaw := make([]byte, 8, 8)
		record := &protocol.Record{Values: make([]*protocol.FieldValue, fieldCount, fieldCount)}
		for i, it := range iterators {
			if rawRecordValues[i] == nil && it.Valid() {
				key := it.Key()
				if len(key) >= 16 {
					// check id is between idStart and idEnd
					id := key[8:16]
					glog.V(2).Infof("fieldId: %v, key %v, start %v, end %v", fieldPairs[i].Id, it.Key(), idStartBytes, idEndBytes)
					if bytes.Equal(key[:8], fieldPairs[i].Id) && bytes.Compare(id, idStartBytes) > -1 && bytes.Compare(id, idEndBytes) < 1 {
						v := it.Value()
						s := key[16:]
						rawRecordValues[i] = &rawRecordValue{id: id, sequence: s, value: v}
						idCompare := bytes.Compare(id, latestIdRaw)
						if idCompare == 1 {
							latestIdRaw = id
							latestSequenceRaw = s
						} else if idCompare == 0 {
							if bytes.Compare(s, latestSequenceRaw) == 1 {
								latestSequenceRaw = s
							}
						}
					}
				}
			}
		}

		for i, iterator := range iterators {
			if rawRecordValues[i] != nil && bytes.Equal(rawRecordValues[i].id, latestIdRaw) &&
				bytes.Equal(rawRecordValues[i].sequence, latestSequenceRaw) {
				var id int64
				var sequence uint32
				isValid = true
				iterator.Next()
				fv := &protocol.FieldValue{}
				err := proto.Unmarshal(rawRecordValues[i].value, fv)
				if err != nil {
					return nil, err
				}
				resultByteCount += len(rawRecordValues[i].value)
				binary.Read(bytes.NewBuffer(rawRecordValues[i].id), binary.BigEndian, &id)

				binary.Read(bytes.NewBuffer(rawRecordValues[i].sequence), binary.BigEndian, &sequence)

				record.Values[i] = fv
				record.Id = &id
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
	ti := self.schema.GetTableInfo(query.Table)
	if ti == nil {
		return -1, fmt.Errorf("Table %s not existed", query.Table)
	}
	condition, idStart, idEnd, err := parser.GetIdCondition(query.WhereExpression)
	if err != nil {
		return -1, err
	}

	// TODO: Make it in parser
	if query.WhereExpression == nil {
		return -1, fmt.Errorf("NO WHERE EXPRESSION IN DELETE")
	}

	conditionFields := query.WhereExpression.GetConditionFields()
	fields := appendReversedIdFieldsIfNeeded(conditionFields)

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
	if !self.schema.Exist(query.Table) {
		return nil, fmt.Errorf("Table %s not existed", query.Table)
	}
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
