package wal

import (
	"os"
)

const (
	defaultLogFilePath        = "/log"
	defaultCheckPointFilePath = "/checkPoint"
)

type WriteAheadLog struct {
	logFiles          []*log
	checkPointFiles   []*checkPointFile
	state             *state
	nextSuffixFileNum int
	closeChan         chan int
	requestChan       chan interface{}
}

func NewWriteAheadLog() *WriteAheadLog {

}
