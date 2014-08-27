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

func logEntryReadHeader(r io.Reader) (*logEntry, error) {
	entry := &logEntry{}
	for _, n := range []*uint32{&entry.requestNumber, &entry.length} {
		if err := binary.Read(r, binary.BigEndian, n); err != nil {
			return nil, err
		}
	}
	return entry, nil
}

type logFile struct {
	file     *os.File
	offset   int
	fielName string
}

func newLogFile(path string, suffix int) *logFile {
	fielName := path.Join(fmt.Sprintf("%s.%d", path, suffix))
	file, err := os.OpenFile(fielName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	return &logFile{
		file:     file,
		fielName: fielName,
	}
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
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	// move to front
	offset, err := file.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	for {
		entry, err := logEntryReadHeader(file)
		if err != nil {
			if err == io.EOF {

			}
			return err
		}

	}
}

func (self *logFile) Append(req *protocol.Request) error {
	bytes, err := proto.Marshal(req)

	entry := &logEntryHeader{
		requestNumber: req.GetRequestNum(),
		length:        uint32(len(bytes)),
	}
	if err = entry.Write(self.file); err != nil {
		glog.Errorf("WRITE LOG HEADER: %s", err.Error())
		return err
	}
	written, err := self.file.Write(bytes)
	if err != nil || written < len(bytes) {
		glog.Errorf("WRITE LOG REQUEST: %s", err.Error())
		return err
	}
	return nil
}
