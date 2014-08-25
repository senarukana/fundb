package wal

import (
	"os"
	"time"
)

const (
	defaultLogFilePath        = "/log"
	defaultCheckPointFilePath = "/checkPoint"
	defaultBufferSize         = 1024
)

type WriteAheadLog struct {
	logFiles          []*log
	checkPointFiles   []*checkPointFile
	state             *state
	nextSuffixFileNum int
	path              string
	closeChan         chan int
	requestChan       chan interface{}
}

func NewWriteAheadLog() *WriteAheadLog {
	wal := &WriteAheadLog{
		requestChan: make(chan interface{}),
		closeChan:   make(chan int),
	}
	go wal.process()
	return wal
}

func (self *WriteAheadLog) Commit() error {
	return nil
}

func (self *WriteAheadLog) Append() error {
	return nil
}

func (self *WriteAheadLog) process() {
	checkPointTick := time.NewTicker(time.Second)
	for {
		select {
		case request := <-self.requestChan:

		case _ = <-checkPointTick:
		case _ = <-self.closeChan:

		}
	}
}

func (self *WriteAheadLog) processCommitRequest() {

}

func (self *WriteAheadLog) processAppendRequest() {

}
