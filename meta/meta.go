package meta

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"sync"
)

type MetaData struct {
	lock    sync.RWMutex
	Version int64
	dbs     map[string]*tableset
}

type tableset struct {
	Tables map[string]*Table
}

func newtableset() *tableset {
	return &tableset{
		Tables: make(map[string]*Table),
	}
}

func (self *tableset) createTable(table *Table) error {
	if tb, _ := self.getTable(table.Name); tb != nil {
		return fmt.Errorf("TABLE %s already existed", table.Name)
	}
	self.Tables[table.Name] = table
	return nil
}

// func (self *tableset) createShard(shard *protocol.Shard) error {
// 	if _, err := self.getTable(shard.GetTableName()); err != nil {
// 		return err
// 	}
// 	tb := self.Tables[shard.GetTableName()]
// 	tb.Shards = append(tb.Shards, shard)
// 	return nil
// }

func (self *tableset) getTable(tbName string) (*Table, error) {
	tb, ok := self.Tables[tbName]
	if !ok {
		return nil, fmt.Errorf("TABLE %s not existed", tb)
	}
	return tb, nil
}

func (self *tableset) listTable() (Tables []*Table) {
	Tables = make([]*Table, 0, len(self.Tables))
	for _, tb := range self.Tables {
		Tables = append(Tables, tb)
	}
	return Tables
}

func NewMeta() *MetaData {
	return &MetaData{
		dbs: make(map[string]*tableset),
	}
}

func (self *MetaData) Save() (data []byte, err error) {
	self.withRLock(func() {
		b := new(bytes.Buffer)
		// fmt.Fprintf(b, "%d\n", self.version)
		err = gob.NewEncoder(b).Encode(&self.dbs)
		if err != nil {
			return
		}
		data = b.Bytes()
	})
	return data, err
}

func Recovery(b []byte) (*MetaData, error) {
	metaData := &MetaData{}
	data, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	err = gob.NewDecoder(buf).Decode(&metaData.dbs)
	return metaData, err
}

func (self *MetaData) withRLock(f func()) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	f()
}

func (self *MetaData) withLock(f func()) {
	self.lock.Lock()
	defer self.lock.Unlock()
	f()
}

func (self *MetaData) getTableSet(dbName string) (*tableset, error) {
	tbSet, ok := self.dbs[dbName]
	if !ok {
		return nil, fmt.Errorf("DB %s not existed", dbName)
	}
	return tbSet, nil
}

func (self *MetaData) CreateDB(dbName string) (err error) {
	self.withLock(func() {
		if _, ok := self.dbs[dbName]; ok {
			err = fmt.Errorf("db %s already existed", dbName)
		}
		self.dbs[dbName] = newtableset()
	})
	return err
}

func (self *MetaData) ListDB() (dbs []string) {
	self.withRLock(func() {
		dbs = make([]string, 0, len(self.dbs))
		for db, _ := range self.dbs {
			dbs = append(dbs, db)
		}
	})
	return dbs
}

// func (self *MetaData) CreateShard(dbName string, shard *protocol.Shard) (err error) {
// 	self.withLock(func() {
// 		var tbSet *tableset
// 		tbSet, err = self.getTableSet(dbName)
// 		if err != nil {
// 			return
// 		}
// 		err = tbSet.createShard(shard)
// 	})
// 	return err
// }

func (self *MetaData) CreateTable(dbName string, table *Table) (err error) {
	self.withLock(func() {
		tbSet, err := self.getTableSet(dbName)
		if err != nil {
			return
		}
		err = tbSet.createTable(table)
	})
	return err
}

func (self *MetaData) GetTable(dbName, tbName string) (tb *Table, err error) {
	self.withRLock(func() {
		tbSet, err := self.getTableSet(dbName)
		if err != nil {
			return
		}
		tb, err = tbSet.getTable(tbName)
	})
	return tb, err
}

func (self *MetaData) GetShard(dbName, tbName string, shardId uint32) (shard *Shard, err error) {
	self.withRLock(func() {
		tbSet, err := self.getTableSet(dbName)
		if err != nil {
			return
		}
		ti, err := tbSet.getTable(tbName)
		if err != nil {
			return
		}
		shard, err = ti.GetShard(shardId)
	})
	return shard, err
}

func (self *MetaData) ListTables(dbName string) (Tables []string, err error) {
	self.withRLock(func() {
		tbSet, err := self.getTableSet(dbName)
		if err != nil {
			return
		}
		for _, metaTable := range tbSet.listTable() {
			Tables = append(Tables, metaTable.Name)
		}
	})
	return Tables, err
}
