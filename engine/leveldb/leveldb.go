package leveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"

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

type Field struct {
	Name string
	Id   []byte
}

type TableInfo struct {
	sync.RWMutex
	fields map[string][]byte
	*protocol.Table
}

type LevelDBEngine struct {
	*levigo.DB
	sync.Mutex
	schema map[string]*TableInfo
}

func NewLevelDBEngine() abstract.StoreEngine {
	leveldbEngine := new(LevelDBEngine)
	return leveldbEngine
}

func (self *LevelDBEngine) initMetaInfo() error {
	self.schema = make(map[string]*TableInfo)

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
				ti := &TableInfo{
					Table:  table,
					fields: make(map[string][]byte),
				}

				for _, column := range ti.GetColumns() {
					ti.fields[column] = genereateColumnId(table.GetName(), column)
				}
				self.schema[table.GetName()] = ti
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
	ti := self.schema[table]
	wo := levigo.NewWriteOptions()
	key := append(LEVELDB_META_PREFIX, genereateMetaTableKey(table)...)
	val, err := proto.Marshal(ti.Table)
	if err != nil {
		return err
	}
	return self.Put(wo, key, val)
}

func (self *LevelDBEngine) getIdForDBTableColumn(table, column string) ([]byte, error) {
	if ti, ok := self.schema[table]; ok {
		if columnId, ok := ti.fields[column]; ok {
			return columnId, nil
		} else {
			ti.Columns = append(ti.Columns, column)
			columnId := genereateColumnId(table, column)
			ti.fields[column] = columnId
			if err := self.updateMeta(table); err != nil {
				return nil, err
			}
			glog.V(3).Infof("Create new column %s\n", column)
			return columnId, nil
		}
	} else {
		return nil, fmt.Errorf("Table %s not existed", table)
	}
}

func (self *LevelDBEngine) CreateTable(table string, idtype parser.TableIdType) error {
	if _, ok := self.schema[table]; ok {
		return fmt.Errorf("Table %s already existed", table)
	}

	var (
		records, size int64
		nextid        int64 = 1
		fields              = []string{"_id"}
	)
	ti := &TableInfo{
		Table:  &protocol.Table{},
		fields: make(map[string][]byte),
	}
	ti.fields["_id"] = genereateColumnId(table, "_id")
	ti.Table.Name = &table
	ti.Table.Records = &records
	ti.Table.Size = &size
	ti.Table.Columns = fields
	if idtype == parser.TABLE_ID_RANDOM {
		ti.Table.Idtype = ID_RANDOM
	} else {
		ti.Table.Idtype = ID_INCREMENT
		ti.Table.Nextid = &nextid
	}
	tableKey := append(LEVELDB_META_PREFIX, []byte(table)...)
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	val, err := proto.Marshal(ti.Table)
	if err == nil {
		if err = self.Put(wo, tableKey, val); err != nil {
			return err
		}
	}
	if err == nil {
		self.schema[table] = ti
	}

	return err
}

func (self *LevelDBEngine) checkTableExistence(table string) error {
	if _, ok := self.schema[table]; !ok {
		return fmt.Errorf("Table %s not existed", table)
	}
	return nil
}

func (self *LevelDBEngine) insertOrDelete(recordList *protocol.RecordList, isDelete bool, ids []int64) error {
	var id int64

	size := 0
	wo := levigo.NewWriteOptions()
	wb := levigo.NewWriteBatch()
	defer wo.Close()
	defer wb.Close()

	ti := self.schema[recordList.GetName()]
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
			columnId, err := self.getIdForDBTableColumn(recordList.GetName(), field)
			if err != nil {
				return err
			}
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
	if !isDelete {
		*ti.Size += int64(size)
		*ti.Records += int64(len(recordList.Values))
	} else {
		*ti.Size -= int64(size)
		*ti.Records -= int64(len(recordList.Values))
	}
	return self.updateMeta(recordList.GetName())
}

func (self *LevelDBEngine) fetch(condition *parser.WhereExpression, table string, fetchFields []string, idStart, idEnd int64, limit int) ([]*protocol.Record, error) {
	if err := self.checkTableExistence(table); err != nil {
		return nil, err
	}

	idStartBytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	idEndBytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(idStartBytesBuffer, binary.BigEndian, idStart)
	binary.Write(idEndBytesBuffer, binary.BigEndian, idEnd)
	idStartBytes := idStartBytesBuffer.Bytes()
	idEndBytes := idEndBytesBuffer.Bytes()

	fields, err := self.getFieldsForTable(table, fetchFields)
	if err != nil {
		return nil, err
	}
	fieldCount := len(fields)
	prefixes := make([][]byte, fieldCount, fieldCount)
	iterators := make([]*levigo.Iterator, fieldCount, fieldCount)
	fieldNames := make([]string, len(fields))

	// start the iterators to go through the series data
	for i, field := range fields {
		fieldNames[i] = field.Name
		prefixes[i] = field.Id
		ro := levigo.NewReadOptions()
		defer ro.Close()
		iterators[i] = self.NewIterator(ro)
		iterators[i].Seek(append(field.Id, idStartBytes...))
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
					glog.V(2).Infof("id %v, start %v, end %v", id, idStartBytes, idEndBytes)
					if bytes.Equal(key[:8], fields[i].Id) && bytes.Compare(id, idStartBytes) > -1 && bytes.Compare(id, idEndBytes) < 1 {
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
	if err := self.checkTableExistence(query.Table); err != nil {
		return -1, err
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
	ti := self.schema[query.Table]
	deleteFields := make([]string, 0, len(ti.fields))
	for field, _ := range ti.fields {
		deleteFields = append(deleteFields, field)
	}
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

func (self *LevelDBEngine) getFieldsForTable(table string, fields []string) ([]*Field, error) {
	res := make([]*Field, 0, len(fields))
	ti := self.schema[table]
	for _, field := range fields {
		if fieldId, ok := ti.fields[field]; ok {
			f := &Field{
				Id:   fieldId,
				Name: field,
			}
			res = append(res, f)
		} else {
			return nil, fmt.Errorf("Unknown field %s", field)
		}
	}
	return res, nil
}

func (self *LevelDBEngine) checkFieldsStar(table string, selectFields []string) ([]string, error) {
	for _, field := range selectFields {
		if field == "*" {
			if len(selectFields) != 1 {
				return nil, fmt.Errorf("* should only be appreared alone in select fields")
			}
			ti := self.schema[table]
			ti.RLock()
			defer ti.RUnlock()
			res := make([]string, 0, len(ti.fields))
			for field, _ := range ti.fields {
				res = append(res, field)
			}
			return res, nil
		}
	}
	return selectFields, nil
}

func (self *LevelDBEngine) Fetch(query *parser.SelectQuery) (*protocol.RecordList, error) {
	if err := self.checkTableExistence(query.Table); err != nil {
		return nil, err
	}
	condition, idStart, idEnd, err := parser.GetIdCondition(query.WhereExpression)
	fetchFields := query.GetSelectAndConditionFields()
	selectFields, err := self.checkFieldsStar(query.Table, query.GetSelectFields())
	if err != nil {
		return nil, err
	}

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
