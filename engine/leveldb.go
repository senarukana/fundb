package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/senarukana/fundb/parser"
	"github.com/senarukana/fundb/protocol"

	"code.google.com/p/goprotobuf/proto"
	"github.com/jmhodges/levigo"
)

const (
	LEVELDB_CACHE_SIZE        = 1024 * 1024 * 16 // 16MB
	LEVELDB_BLOCK_SIZE        = 256 * 1024
	LEVELDB_BLOOM_FILTER_BITS = 64
	MAX_POINTS_TO_SCAN        = 1000000
	SEPERATOR                 = '|'
	MAX_RECORD_NUM            = 100000
	MAX_FETCH_SIZE            = 1024 * 1024 // 1MB
)

var (
	EMPTYBYTE                  = []byte(" ")
	LEVELDB_META_PREFIX        = []byte("META")
	LEVELDB_TABLE_PREFIX       = []byte("META_TABLE")
	LEVELDB_TABLEFIELDS_PREFIX = []byte("META_FIELD")
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

var (
	ID_INCREMENT = []byte{0x00, 0x00}
	ID_TIMESTAMP = []byte{0x00, 0x01}
)

type TableInfo struct {
	sync.Mutex
	columns map[string][]byte
	IdType  []byte
	nextId  int64
	records int64
	size    int64 // bit
}

type LevelDBEngine struct {
	*levigo.DB
	sync.Mutex
	schema map[string]*TableInfo
}

func NewLevelDBEngine() storeEngine {
	leveldbEngine := new(LevelDBEngine)
	return leveldbEngine
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

	return nil
}

func (self *LevelDBEngine) genereateTableKey(table string) []byte {
	// check if table exists
	b := []byte(table)
	tableKey := append(LEVELDB_TABLE_PREFIX, b...)
	return tableKey
}

func (self *LevelDBEngine) genereateColumnKey(table, column string) []byte {
	// check if table exists
	b := []byte(fmt.Sprintf("%s%c%s", table, SEPERATOR, column))
	columnKey := append(LEVELDB_TABLEFIELDS_PREFIX, b...)
	return columnKey
}

func (self *LevelDBEngine) getIdForDBTableColumn(table, column string) ([]byte, error) {
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	defer wo.Close()
	tableInfo := self.schema[table]
	if key, ok := tableInfo.columns[column]; ok {
		return key, nil
	} else {
		key := self.genereateColumnKey(table, column)
		if err := self.Put(wo, key, EMPTYBYTE); err != nil {
			return nil, err
		}

		self.schema[table].columns[column] = key
		return key, nil
	}
}

func (self *LevelDBEngine) Insert(recordList *protocol.RecordList) error {
	wo := levigo.NewWriteOptions()
	wb := levigo.NewWriteBatch()
	defer wo.Close()
	defer wb.Close()
	if _, ok := self.schema[recordList.GetName()]; !ok {
		wo.SetSync(true)
		if err := self.Put(wo, self.genereateTableKey(recordList.GetName()), ID_INCREMENT); err != nil {
			return err
		}
		self.schema[recordList.GetName()] = &TableInfo{
			IdType: ID_INCREMENT,
			nextId: int64(1),
		}
	}

	tableInfo := self.schema[recordList.GetName()]
	tableInfo.Lock()
	defer tableInfo.Unlock()
	for fieldIndex, field := range recordList.Fields {
		id, err := self.getIdForDBTableColumn(recordList.GetName(), field)
		if err != nil {
			return err
		}
		for _, record := range recordList.Values {
			idBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			sequenceNumberBuffer := bytes.NewBuffer(make([]byte, 0, 4))
			if bytes.Compare(tableInfo.IdType, ID_INCREMENT) == 0 {
				binary.Write(idBuffer, binary.BigEndian, tableInfo.nextId)
			} else {
				binary.Write(idBuffer, binary.BigEndian, time.Now().UnixNano())
			}
			binary.Write(sequenceNumberBuffer, binary.BigEndian, record.SequenceNum)
			recordKey := append(append(id, idBuffer.Bytes()...), sequenceNumberBuffer.Bytes()...)
			fmt.Println(record.Values[fieldIndex].String())
			data, err := proto.Marshal(record.Values[fieldIndex])
			if err != nil {
				return err
			}
			wb.Put(recordKey, data)
			tableInfo.nextId++
		}
	}
	return self.Write(wo, wb)
}

func (self *LevelDBEngine) getFieldsForTable(table string, columns []string) (fields []*Field, err error) {
	fields = make([]*Field, 0, len(columns))
	for _, column := range columns {
		if fieldId, ok := self.schema[table].columns[column]; ok {
			field := &Field{
				Id:   fieldId,
				Name: column,
			}
			fields = append(fields, field)
		} else {
			return nil, fmt.Errorf("Unknown column %s", column)
		}
	}
	return fields, nil
}

func (self *LevelDBEngine) executeQueryForTable(table string, columns []string,
	idStart, idEnd int, whereExpr *parser.WhereExpression, limit int) (*protocol.RecordList, error) {

	idStartBytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	idEndBytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(idStartBytesBuffer, binary.BigEndian, idStart)
	binary.Write(idEndBytesBuffer, binary.BigEndian, idEnd)
	idStartBytes := idStartBytesBuffer.Bytes()
	idEndBytes := idEndBytesBuffer.Bytes()

	fields, err := self.getFieldsForTable(table, columns)
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
		iterators[i].Seek(field.Id)
	}

	result := &protocol.RecordList{Name: &table, Fields: fieldNames, Values: make([]*protocol.Record, 0)}
	rawRecordValues := make([]*rawRecordValue, fieldCount, fieldCount)
	isValid := true

	if limit == 0 {
		limit = MAX_RECORD_NUM
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
				var id, sequence int64
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
				seq := uint32(sequence)

				record.Values[i] = fv
				record.SequenceNum = &seq
				rawRecordValues[i] = nil
			}
		}
		if isValid {
			limit--
			result.Values = append(result.Values, record)

			// add byte count for the timestamp and the sequence
			resultByteCount += 16

			// check if we should send the batch along
			if resultByteCount > MAX_FETCH_SIZE {
				break
			}
		}
		if limit < 1 {
			break
		}
	}
	// filteredResult, _ := Filter(whereExpr, result)
	return result, nil
}

func (self *LevelDBEngine) Close() error {
	return self.Close()
}

func (self *LevelDBEngine) DropDatabase(database string) error {
	return nil
}
