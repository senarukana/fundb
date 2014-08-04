package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/senarukana/fundb/protocol"

	"code.google.com/p/goprotobuf/proto"
	"github.com/jmhodges/levigo"
)

const (
	LevelDBCacheSize   = 1024 * 1024 * 16 // 16MB
	LevelDBBlockSize   = 256 * 1024
	LevelDBBloomFilter = 64
	MAX_POINTS_TO_SCAN = 1000000
)

type LevelDBEngine struct {
	*levigo.DB
}

func NewLevelDBEngine() storeEngine {
	leveldbEngine := new(LevelDBEngine)
	return leveldbEngine
}

func (self *LevelDBEngine) Init(dataPath string) (err error) {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(LevelDBCacheSize))
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(LevelDBBlockSize)
	filter := levigo.NewBloomFilter(LevelDBBloomFilter)
	opts.SetFilterPolicy(filter)
	self.DB, err = levigo.Open(dataPath, opts)
	if err != nil {
		return err
	}

	return nil
}

func (self *LevelDBEngine) Insert(database string, recordList *protocol.RecordList) error {
	wo := levigo.NewWriteOptions()
	wb := levigo.NewWriteBatch()
	defer wo.Close()
	defer wb.Close()
	for fieldIndex, field := range recordList.Fields {
		id := getIdForDBTableColumn(database, *recordList.Name, field)
		for _, record := range recordList.Values {
			timestampBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			sequenceNumberBuffer := bytes.NewBuffer(make([]byte, 0, 4))
			binary.Write(timestampBuffer, binary.BigEndian, time.Now().UnixNano())
			binary.Write(sequenceNumberBuffer, binary.BigEndian, record.SequenceNum)
			recordKey := append(append(id, timestampBuffer.Bytes()...), sequenceNumberBuffer.Bytes()...)
			fmt.Println(record.Values[fieldIndex].String())
			data, err := proto.Marshal(record.Values[fieldIndex])
			if err != nil {
				return err
			}
			wb.Put(recordKey, data)
		}
	}
	return self.Write(wo, wb)
}

func (self *LevelDBEngine) Close() error {
	return self.Close()
}

func (self *LevelDBEngine) DropDatabase(database string) error {
	return nil
}
