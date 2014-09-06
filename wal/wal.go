package wal

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/senarukana/fundb/protocol"

	"code.google.com/p/goprotobuf/proto"
	"github.com/golang/glog"
)

const (
	defaultLogDir                  = "wal"
	logPrefix                      = "log."
	checkPointPrefix               = "checkpoint."
	defaultReplayBuffer            = 10
	defaultBufferSize              = 10 // 1024
	defaultCheckpointAfterRequests = 10 // 1024
	defaultBookmarkAfterRequests   = 20 // 1024 * 32
	defaultRotateThreshold         = 100
)

type WriteAheadLog struct {
	logFiles        []*logFile
	checkPointFiles []*checkPointFile
	state           *state
	logdir          string
	closeChan       chan bool
	completeChan    chan bool
	requestChan     chan interface{}

	requestsSinceLastCheckpoint uint32
	requestsSinceLastBookmark   uint32
	requestsSinceLastFlush      uint32
	requestsSinceRotation       uint32
}

func NewWriteAheadLog() (*WriteAheadLog, error) {
	wal := &WriteAheadLog{
		requestChan:  make(chan interface{}, defaultBufferSize),
		closeChan:    make(chan bool),
		completeChan: make(chan bool),
		logdir:       defaultLogDir,
	}

	_, err := os.Stat(wal.logdir)
	if os.IsNotExist(err) {
		err = os.Mkdir(wal.logdir, 0755)
	}
	if err != nil {
		return nil, err
	}

	wal.state, err = newState(path.Join(wal.logdir, "bookmark"))
	if err != nil {
		return nil, err
	}

	glog.Errorln(wal.state)

	dir, err := os.Open(wal.logdir)
	if err != nil {
		return nil, err
	}

	fileNames, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	for _, name := range fileNames {
		if !strings.HasPrefix(name, "log.") {
			if strings.HasPrefix(name, "checkpoint.") {
				continue
			}
			glog.Warning("INVALID FILE NAME %s in WAL DIR: %s", name, wal.logdir)
			continue
		}
		suffixString := strings.TrimLeft(path.Base(name), logPrefix)
		suffix, err := strconv.Atoi(suffixString)
		if err != nil {
			glog.Warning("INVALID FILE NAME %s in WAL DIR: %s", name, wal.logdir)
			continue
		}
		if err = wal.openLog(suffix); err != nil {
			return nil, err
		}
	}

	sort.Sort(sortableLogSlice{wal.logFiles, wal.checkPointFiles})

	for _, log := range wal.logFiles {
		glog.Errorf(log.file.Name())
	}

	go wal.process()
	return wal, nil
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

func (self *WriteAheadLog) Close(sync bool) {
	self.closeChan <- sync
	<-self.completeChan
}

func (self *WriteAheadLog) process() {
	checkPointTick := time.NewTicker(time.Second)
	deleteObsoleteTick := time.NewTicker(time.Microsecond * 10)
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
		case sync := <-self.closeChan:
			if sync {
				if err := self.flush(); err != nil {
					glog.Errorf("FLUSH TO LOGFILE: %s", err.Error())
				}
				if err := self.checkpoint(); err != nil {
					glog.Errorf("CHECKPOINT: %s", err.Error())
				}
				if err := self.bookmark(); err != nil {
					glog.Errorf("BOOKMARK: %s", err.Error())
				}
			}
			goto exit
		}
	}
exit:
	glog.Errorf("WAL CLOSING")
	close(self.completeChan)
}

func (self *WriteAheadLog) processAppendRequest(req *appendRequest) {
	var err error
	var logfile *logFile
	requestNum := self.state.GetNextRequestNum()
	resp := &response{
		requestNum: requestNum,
	}
	req.request.RequestNum = proto.Uint32(requestNum)
	defer func() {
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
	self.requestsSinceRotation++
	if err = self.rotate(); err != nil {
		return
	}
	self.conditionalIndex()
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
	if index == 0 {
		return
	}
	glog.Errorf("DELETE OBSOLETE FILE TO %d", index)
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
	if err := self.openLog(int(self.state.GetNextFileNum())); err != nil {
		return err
	}
	self.state.CurrentFileOffset = 0
	return nil
}

func (self *WriteAheadLog) openLog(suffix int) error {
	logPath := path.Join(self.logdir, fmt.Sprintf("%s%d", logPrefix, suffix))
	ckPath := path.Join(self.logdir, fmt.Sprintf("%s%d", checkPointPrefix, suffix))
	log, err := newLogFile(logPath)
	if err != nil {
		return err
	}
	checkPoint, err := newCheckPointFile(ckPath)
	if err != nil {
		return err
	}
	self.logFiles = append(self.logFiles, log)
	self.checkPointFiles = append(self.checkPointFiles, checkPoint)
	return nil
}

func (self *WriteAheadLog) Recover(do func(request *protocol.Request) error) error {
	return self.recover(do)
}

func (self *WriteAheadLog) recover(do func(request *protocol.Request) error) error {
	recoverIdx := -1
	for i, ckFile := range self.checkPointFiles {
		if ckFile.getRequestOffset(self.state.CurrentCommitNum) != -1 {
			recoverIdx = i
			break
		}
	}
	glog.Errorf("RECOVER FROM IDX %d", recoverIdx)
	if recoverIdx == -1 {
		glog.Fatalln("CAN'T FIND REQUEST NUM IN CHECKPOINT FILES")
	}
	ckfile := self.checkPointFiles[recoverIdx]
	offset := ckfile.getRequestOffset(self.state.CurrentCommitNum)
	glog.Errorf("RECOVER FILE: %s FROM OFFSET %d", self.logFiles[recoverIdx].file.Name(), offset)

	// in case the log file is rotated or deleted
	logFiles := make([]*logFile, len(self.logFiles))
	copy(logFiles, self.logFiles)

	for i := recoverIdx; i < len(logFiles); i++ {
		logfile := logFiles[i]
		if i > recoverIdx {
			offset = -1
		}
		replayChan, stopChan := logfile.replay(offset, self.state.CurrentCommitNum)
		count := 0

		for {
			replay := <-replayChan
			if replay == nil {
				glog.Errorf("REPLAY FROM FILE %s COMPLETE, COUNT = %d", logfile.file.Name(), count)
				close(stopChan)
				break
			}
			if replay.err != nil {
				return replay.err
			}
			if err := do(replay.request); err != nil {
				glog.Errorf("DO REPLAY ERROR: %s", err.Error())
				stopChan <- true
				return err
			}
			count++
		}
	}
	return nil
}

func (self *WriteAheadLog) rotate() error {
	if self.requestsSinceRotation < defaultRotateThreshold {
		return nil
	}
	self.requestsSinceRotation = 0
	if self.requestsSinceLastCheckpoint > 0 {
		if err := self.checkpoint(); err != nil {
			return err
		}
	}
	if self.requestsSinceLastBookmark > 0 {
		if err := self.bookmark(); err != nil {
			return err
		}
	}
	lastLogFile := self.logFiles[len(self.logFiles)-1]
	lastCheckpointFile := self.checkPointFiles[len(self.checkPointFiles)-1]

	glog.V(2).Infof("ROTATE LOG FILE %s", lastLogFile.filePath)
	lastLogFile.close()
	lastCheckpointFile.close()
	return self.createNewLog()
}

func (self *WriteAheadLog) conditionalIndex() error {
	if self.requestsSinceLastCheckpoint >= defaultCheckpointAfterRequests {
		if err := self.checkpoint(); err != nil {
			return err
		}
	}

	if self.requestsSinceLastBookmark >= defaultBookmarkAfterRequests {
		if err := self.bookmark(); err != nil {
			return err
		}
	}
	return nil
}

func (self *WriteAheadLog) flush() error {
	glog.Errorf("Fsyncing the log file to disk")
	self.requestsSinceLastFlush = 0
	lastIndex := len(self.logFiles) - 1
	if err := self.logFiles[lastIndex].sync(); err != nil {
		return err
	}
	if err := self.checkPointFiles[lastIndex].sync(); err != nil {
		return err
	}
	return nil
}

func (self *WriteAheadLog) bookmark() error {
	if err := self.state.Sync(); err != nil {
		glog.Errorf("SYNC BOOKMARK %s", err.Error())
		return err
	}
	self.requestsSinceLastBookmark = 0
	return nil
}

func (self *WriteAheadLog) checkpoint() error {
	checkPointFile := self.checkPointFiles[len(self.checkPointFiles)-1]
	ck := &checkPoint{
		RequestNumStart: self.state.CurrentRequestNum - 1 + self.requestsSinceLastCheckpoint,
		RequestNumEnd:   self.state.CurrentRequestNum,
		FirstOffset:     checkPointFile.getLastOffset(),
		LastOffset:      self.state.CurrentFileOffset,
	}
	if err := checkPointFile.append(ck); err != nil {
		return err
	}
	self.requestsSinceLastCheckpoint = 0
	return nil
}

// for test only
func (self *WriteAheadLog) truncate() {
	os.RemoveAll(self.logdir)
}
