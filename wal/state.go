package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/golang/glog"
)

type state struct {
	path              string
	CurrentRequestNum uint32
	CurrentCommitNum  uint32
	CurrentFileNum    int32
	CurrentFileOffset int64
}

func (self *state) String() string {
	return fmt.Sprintf("STATE: [RequestNum:%d, CommitNum:%d, FileNum:%d, FileOffset: %d]",
		self.CurrentRequestNum, self.CurrentCommitNum, self.CurrentFileNum, self.CurrentFileOffset)
}

func newState(path string) (*state, error) {
	file, err := os.Open(path)
	s := &state{
		path: path,
	}
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if err = s.read(file); err != nil {
		return nil, err
	}
	return s, nil
}

func (self *state) GetNextRequestNum() uint32 {
	self.CurrentRequestNum++
	return self.CurrentRequestNum
}

func (self *state) GetNextFileNum() int32 {
	self.CurrentFileNum++
	return self.CurrentFileNum
}

func (self *state) Commit(requestNum uint32) {
	self.CurrentCommitNum = requestNum
}

func (self *state) Sync() error {
	glog.V(4).Infof("STATE SYNC: %s", self)
	newName := self.path + ".new"
	newFile, err := os.OpenFile(newName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if err := self.write(newFile); err != nil {
		return err
	}
	if err := newFile.Sync(); err != nil {
		return err
	}
	if err := newFile.Close(); err != nil {
		return err
	}

	os.Remove(self.path)
	return os.Rename(newName, self.path)
}

func (self *state) write(w io.Writer) error {

	if err := binary.Write(w, binary.BigEndian, self.CurrentRequestNum); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, self.CurrentFileNum); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, self.CurrentFileOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, self.CurrentCommitNum); err != nil {
		return err
	}
	return nil
}

func (self *state) read(r io.Reader) error {
	if err := binary.Read(r, binary.BigEndian, &self.CurrentRequestNum); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &self.CurrentFileNum); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &self.CurrentFileOffset); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &self.CurrentCommitNum); err != nil {
		return err
	}
	return nil
}
