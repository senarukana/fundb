package wal

import (
	"time"

	"github.com/senarukana/fundb/protocol"

	"code.google.com/p/goprotobuf/proto"
	"github.com/golang/glog"
)

const (
	defaultLogFilePath             = "/log"
	defaultCheckPointFilePath      = "/checkPoint"
	defaultBufferSize              = 1024
	defaultCheckpointAfterRequests = 1024
	defaultBookmarkAfterRequests   = 1024 * 32
	defaultRotateThreshold         = 1024
)

type WriteAheadLog struct {
	logFiles        []*logFile
	checkPointFiles []*checkPointFile
	state           *state
	logPath         string
	checkPointPath  string
	closeChan       chan int
	requestChan     chan interface{}

	requestsSinceLastCheckpoint uint32
	requestsSinceLastBookmark   uint32
	requestsSinceLastFlush      uint32
	requestsSinceRotation       uint32
}

func NewWriteAheadLog() *WriteAheadLog {
	wal := &WriteAheadLog{
		requestChan:    make(chan interface{}, defaultBufferSize),
		closeChan:      make(chan int),
		logPath:        defaultLogFilePath,
		checkPointPath: defaultCheckPointFilePath,
	}
	go wal.process()
	return wal
}

func (self *WriteAheadLog) Commit(requestNum uint32) error {
	self.state.Commit(requestNum)
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
	deleteObsoleteTick := time.NewTicker(time.Minute)
	for {
		select {
		case req := <-self.requestChan:
			switch t := req.(type) {
			case *appendRequest:
				self.processAppendRequest(t)
			}
		case _ = <-checkPointTick.C:
			self.checkpoint()
		case _ = <-deleteObsoleteTick.C:
			self.deleteObsoleteFiles()
		case _ = <-self.closeChan:

		}
	}
}

func (self *WriteAheadLog) processAppendRequest(req *appendRequest) {
	var err error
	var logfile *logFile
	defer func() {
		requestNum := self.state.GetNextRequestNum()
		resp := &response{
			requestNum: requestNum,
		}
		req.request.RequestNum = proto.Uint32(requestNum)
		if err != nil {
			resp.err = err
		}
		req.respChan <- resp
	}()

	if len(self.logFiles) == 0 {
		if err = self.createNewLog(); err != nil {
			return
		}
	}
	logfile = self.logFiles[len(self.logFiles)-1]
	if err = logfile.append(req.request); err != nil {
		return
	}

	self.state.CurrentFileOffset = logfile.offset()
	self.requestsSinceLastCheckpoint++
	self.requestsSinceLastBookmark++
	self.requestsSinceLastFlush++

	err = self.rotate()
	return
}

func (self *WriteAheadLog) deleteObsoleteFiles() {
	var index int
	lastCommitNum := self.state.CurrentCommitNum

	for i, cf := range self.checkPointFiles {
		if cf.getRequestOffset(lastCommitNum) != -1 {
			index = i
			break
		}
	}
	// there is no obsolete files
	if index == len(self.checkPointFiles)-1 {
		return
	}

	var obsoleteLogFiles []*logFile
	var obsoleteCheckPointFiles []*checkPointFile

	obsoleteLogFiles, self.logFiles = self.logFiles[:index], self.logFiles[index:]
	obsoleteCheckPointFiles, self.checkPointFiles = self.checkPointFiles[:index], self.checkPointFiles[index:]

	for i := 0; i < index; i++ {
		obsoleteLogFiles[i].close()
		obsoleteLogFiles[i].delete()

		obsoleteCheckPointFiles[i].close()
		obsoleteCheckPointFiles[i].delete()
	}
}

func (self *WriteAheadLog) createNewLog() error {
	fileNum := int(self.state.GetNextFileNum())
	log, err := newLogFile(self.logPath, fileNum)
	if err != nil {
		return err
	}
	checkPoint, err := newCheckPointFile(self.checkPointPath, fileNum)
	if err != nil {
		return err
	}
	self.logFiles = append(self.logFiles, log)
	self.checkPointFiles = append(self.checkPointFiles, checkPoint)
	return nil
}

func (self *WriteAheadLog) rotate() error {
	if self.requestsSinceRotation < defaultRotateThreshold {
		return nil
	}
	self.requestsSinceRotation = 0
	if self.requestsSinceLastCheckpoint > 0 {
		self.checkpoint()
	}
	lastLogFile := self.logFiles[len(self.logFiles)-1]
	lastCheckpointFile := self.checkPointFiles[len(self.checkPointFiles)-1]

	glog.V(2).Infof("ROTATE LOG FILE %s", lastLogFile.fileName)
	lastLogFile.close()
	lastCheckpointFile.close()
	return self.createNewLog()
}

func (self *WriteAheadLog) conditionalIndex() {
	if self.requestsSinceLastCheckpoint >= defaultCheckpointAfterRequests {
		self.checkpoint()
	}

	if self.requestsSinceLastBookmark >= defaultBookmarkAfterRequests {
		self.bookmark()
	}
}

func (self *WriteAheadLog) bookmark() {
	if err := self.state.Sync(); err != nil {
		glog.Errorf("SYNC BOOKMARK %s", err.Error())
	}
	self.requestsSinceLastBookmark = 0
}

func (self *WriteAheadLog) checkpoint() {
	checkPointFile := self.checkPointFiles[len(self.checkPointFiles)-1]
	checkPointFile.append(&checkPoint{
		RequestNumStart: self.state.CurrentRequestNum - 1 + self.requestsSinceLastCheckpoint,
		RequestNumEnd:   self.state.CurrentRequestNum,
		FirstOffset:     checkPointFile.getLastOffset(),
		LastOffset:      self.state.CurrentFileOffset,
	})
	self.requestsSinceLastCheckpoint = 0
}
