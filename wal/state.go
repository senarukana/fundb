package wal

type state struct {
	Path              string
	NextRequestNum    uint64
	CurrentFileNum    int32
	CurrentFileOffset int32
}

func newState(path string) *state {
	return &state{
		Path: path,
	}
}

func (self *state) GetNextRequestNum() uint32 {
	num := self.NextRequestNum
	self.NextRequestNum++
	return num
}
