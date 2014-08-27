package wal

import (
	"fmt"
	"os"

	"github.com/golang/glog"
)

const (
	SEPERATOR = '|'
)

type checkPoint struct {
	RequestNumStart uint64
	RequestNumEnd   uint64
	FirstOffset     int64
	LastOffset      int64
}

type checkPointFile struct {
	file          *os.File
	checkPoints   []*checkPoint
	logFileSuffix int
	offset        int
	dir           string
}

func newCheckPointManager(dir string) *checkPointManager {
	return nil
}

func (self *checkPointFile) Append(ck *checkPoint) {
	_, err := fmt.Fprintf(self.file, "%d.%d.%d.%d", ck.RequestNumStart, ck.RequestNumEnd, ck.FirstOffset, ck.LastOffset)
	if err != nil {
		glog.Errorf("APPEND CHECKPOINT: %s", err.Error())
		return
	}
	self.checkPoints = append(self.checkPoints, ck)
}

func (self *checkPointFile) GetLastOffset() int64 {
	if len(self.checkPoints) == 0 {
		return 0
	}
	return self.checkPoints[len(self.checkPoints)-1].LastOffset
}
