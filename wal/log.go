package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

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
	fileName string
	dir      string
}

func newLogFile(dir string, suffix int) (*logFile, error) {
	fileName := path.Join(fmt.Sprintf("%s.%d", dir, suffix))
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	log := &logFile{
		file:     file,
		fileName: fileName,
		dir:      dir,
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
	glog.V(2).Info("DELETE LOGFILE %s", self.fileName)
	os.Remove(self.fileName)
}

func (self *logFile) sync() {
	self.file.Sync()
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

	entry := &logEntryHeader{
		requestNumber: req.GetRequestNum(),
		length:        uint32(len(data)),
	}

	err = entry.Write(self.file)
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
