package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/senarukana/fundb/protocol"

	"code.google.com/p/goprotobuf/proto"
	"github.com/golang/glog"
)

type logEntryHeader struct {
	requestNumber uint32
	length        uint32
}

func (self *logEntryHeader) Write(w io.Writer) error {
	for _, n := range []uint32{self.requestNumber, self.length} {
		if err := binary.Write(w, binary.BigEndian, n); err != nil {
			return err
		}
	}
	return nil
}

func nextLogEntryHeader(r io.Reader) (int, *logEntryHeader, error) {
	entry := &logEntryHeader{}
	size := 0
	for _, n := range []*uint32{&entry.requestNumber, &entry.length} {
		if err := binary.Read(r, binary.BigEndian, n); err != nil {
			return size, nil, err
		}
		size += 4
	}
	return size, entry, nil
}

type logFile struct {
	file     *os.File
	suffix   int
	filePath string
}

func newLogFile(filePath string) (*logFile, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	suffixString := strings.TrimLeft(path.Base(file.Name()), logPrefix)
	suffix, err := strconv.Atoi(suffixString)
	if err != nil {
		return nil, err
	}
	log := &logFile{
		file:     file,
		filePath: filePath,
		suffix:   suffix,
	}
	if err = log.check(); err != nil {
		return nil, err
	}
	return log, nil
}

func (self *logFile) dupLogFile() (*os.File, error) {
	return os.OpenFile(self.file.Name(), os.O_RDWR, 0)
}

// sanity check for logfile
func (self *logFile) check() error {
	file, err := self.dupLogFile()
	if err != nil {
		return err
	}
	defer file.Close()

	// move to front
	offset, err := file.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	for {
		n, entry, err := nextLogEntryHeader(file)
		if err != nil {
			if err == io.EOF {
				return self.file.Truncate(offset)
			}
			return err
		}
		if _, err = file.Seek(int64(entry.length), os.SEEK_CUR); err != nil {
			return err
		}
		offset += int64(n) + int64(entry.length)
	}
}

func (self *logFile) close() {
	self.file.Close()
}

func (self *logFile) delete() {
	glog.V(2).Info("DELETE LOGFILE %s", self.filePath)
	os.Remove(self.filePath)
}

func (self *logFile) sync() error {
	return self.file.Sync()
}

func (self *logFile) offset() int64 {
	offset, _ := self.file.Seek(0, os.SEEK_CUR)
	return offset
}

func (self *logFile) append(req *protocol.Request) error {
	data, err := proto.Marshal(req)
	if err != nil {
		glog.Errorf("MARSHAL REQUEST: %s", err.Error())
		return err
	}

	hdr := &logEntryHeader{
		requestNumber: req.GetRequestNum(),
		length:        uint32(len(data)),
	}

	err = hdr.Write(self.file)
	if err != nil {
		glog.Errorf("WRITE LOG HEADER: %s", err.Error())
		return err
	}
	written, err := self.file.Write(data)
	if err != nil || written < len(data) {
		glog.Errorf("WRITE LOG REQUEST: %s", err.Error())
		return err
	}
	return nil
}

func (self *logFile) skipToRequestNum(file *os.File, requestNum uint32) error {
	for {
		n, hdr, err := nextLogEntryHeader(file)
		if err != nil {
			return err
		}
		if hdr.requestNumber < requestNum {
			if _, err = file.Seek(int64(hdr.length), os.SEEK_CUR); err != nil {
				return err
			}
			continue
		}

		// move back to the entry header
		_, err = file.Seek(int64(-n), os.SEEK_CUR)
		return err
	}
}

func (self *logFile) skip(file *os.File, offset int64, requestNum uint32) error {
	if offset == -1 {
		return nil
	}
	if _, err := file.Seek(offset, os.SEEK_CUR); err != nil {
		return err
	}
	if err := self.skipToRequestNum(file, requestNum); err != nil {
		return err
	}
	return nil
}

func (self *logFile) replay(offset int64, requestNum uint32) (replayChan chan *replayRequest, stopChan chan bool) {
	replayChan = make(chan *replayRequest, defaultReplayBuffer)
	stopChan = make(chan bool)
	go func() {
		var err error
		var file *os.File
		var req protocol.Request

		defer func() {
			file.Close()
			if err != nil {
				glog.Errorf("REPLAY: %s", err.Error())
				replay := &replayRequest{
					err: err,
				}
				sendOrStop(replay, replayChan, stopChan)
			}
			close(replayChan)
		}()

		if file, err = self.dupLogFile(); err != nil {
			return
		}
		glog.V(2).Infof("Replay from offset %d", offset)
		if err = self.skip(file, offset, requestNum); err != nil {
			glog.Errorf("REPLAY SKIP: %s", err.Error())
			return
		}

		for {
			_, hdr, e := nextLogEntryHeader(file)
			if e != nil {
				if e == io.EOF {
					err = nil
				} else {
					err = e
				}
				return
			}
			data := make([]byte, hdr.length)
			if _, err = file.Read(data); err != nil {
				return
			}
			if err = proto.Unmarshal(data, &req); err != nil {
				return
			}
			if sendOrStop(newReplayRequest(&req), replayChan, stopChan) {
				return
			}
		}
	}()

	return replayChan, stopChan
}

func sendOrStop(req *replayRequest, replayChan chan *replayRequest, stopChan chan bool) bool {
	select {
	case replayChan <- req:
	case _, ok := <-stopChan:
		glog.Errorf("Stopping replay")
		return ok
	}
	return false
}
