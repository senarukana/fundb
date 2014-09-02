package wal

import (
	"encoding/binary"
	"io"
	"os"
)

type state struct {
	path              string
	CurrentRequestNum uint32
	CurrentCommitNum  uint32
	CurrentFileNum    int32
	CurrentFileOffset int64
}

func newState(path string) *state {
	return &state{
		path: path,
	}
}

func (self *state) GetNextRequestNum() uint32 {
	num := self.CurrentRequestNum
	self.CurrentRequestNum++
	return num
}

func (self *state) GetNextFileNum() int32 {
	num := self.CurrentFileNum
	self.CurrentFileNum++
	return num
}

func (self *state) Commit(requestNum uint32) {
	self.CurrentCommitNum = requestNum
}

func (self *state) Sync() error {
	newName := self.path + ".new"
	newFile, err := os.OpenFile(newName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if _, err := newFile.Seek(0, os.SEEK_SET); err != nil {
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
