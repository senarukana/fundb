package wal

import (
	"os"
	"time"

	"github.com/senarukana/fundb/protocol"

	"code.google.com/p/goprotobuf/proto"
)

const (
	defaultLogFilePath        = "/log"
	defaultCheckPointFilePath = "/checkPoint"
	defaultBufferSize         = 1024
)

type WriteAheadLog struct {
	logFiles        []*log
	checkPointFiles []*checkPointFile
	state           *state
	path            string
	closeChan       chan int
	requestChan     chan interface{}

	nextSuffixLogNum            int
	requestsSinceLastCheckpoint uint32
}

func NewWriteAheadLog() *WriteAheadLog {
	wal := &WriteAheadLog{
		requestChan: make(chan interface{}, defaultBufferSize),
		closeChan:   make(chan int),
	}
	go wal.process()
	return wal
}

func (self *WriteAheadLog) Commit(requestNum uint32) error {
	return nil
}

func (self *WriteAheadLog) Append(req *protocol.Request) (uint32, error) {
	respChan := make(chan *response)
	self.requestChan <- &appendRequest{
		request:  req,
		respChan: respChan,
	}
	resp := <-respChan
	return resp.requestNum, resp.err
}

func (self *WriteAheadLog) process() {
	checkPointTick := time.NewTicker(time.Second)
	for {
		select {
		case req := <-self.requestChan:
			switch req.(type) {
			case *appendRequest:
				self.processAppendRequest(req)
			}
		case _ = <-checkPointTick:
			self.checkpoint()
		case _ = <-self.closeChan:

		}
	}
}

func (self *WriteAheadLog) processCommitRequest() {

}

func (self *WriteAheadLog) processAppendRequest(req *appendRequest) {
	requestNum := self.state.GetNextRequestNum()
	req.request.RequestNum = proto.Uint32(requestNum)

	if len(self.logFiles) == 0 {

	}
}

func (self *WriteAheadLog) checkpoint() {
	checkPointFile := self.checkPointFiles[len(self.checkPointFiles)-1]
	checkPointFile.Append(&checkPoint{
		RequestNumStart: self.state.NextRequestNum - 1 + self.requestsSinceLastCheckpoint,
		RequestNumEnd:   self.state.NextRequestNum,
		FirstOffset:     checkPointFile.GetLastOffset(),
		LastOffset:      self.state.CurrentFileOffset,
	})
	self.requestsSinceLastCheckpoint = 0
}
