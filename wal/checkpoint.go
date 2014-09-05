package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

const (
	SEPERATOR = '|'
)

type checkPoint struct {
	RequestNumStart uint32
	RequestNumEnd   uint32
	FirstOffset     int64
	LastOffset      int64
}

type checkPointFile struct {
	file        *os.File
	checkPoints []*checkPoint
	filePath    string
	suffix      int
	offset      int
}

func newCheckPointFile(filePath string) (*checkPointFile, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	suffixString := strings.TrimLeft(path.Base(file.Name()), checkPointPrefix)
	suffix, err := strconv.Atoi(suffixString)
	if err != nil {
		return nil, err
	}

	ck := &checkPointFile{
		file:     file,
		filePath: filePath,
		suffix:   suffix,
	}
	if err = ck.recover(); err != nil {
		return nil, err
	}
	return ck, nil
}

func (self *checkPointFile) recover() error {
	content, err := ioutil.ReadAll(self.file)
	if err != nil {
		return err
	}

	lines := strings.Split(string(content), "\n")
	checkPoints := make([]*checkPoint, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}
		fields := strings.Split(line, ".")
		if len(fields) != 4 {
			return fmt.Errorf("INVALID checkpoint format: %s", line)
		}
		firstRequestNumber, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return err
		}
		lastRequestNumber, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return err
		}
		firstOffset, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return err
		}
		lastOffset, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil {
			return err
		}
		checkPoints = append(checkPoints, &checkPoint{
			uint32(firstRequestNumber),
			uint32(lastRequestNumber),
			firstOffset,
			lastOffset,
		})
	}
	self.checkPoints = checkPoints
	return nil
}

func (self *checkPointFile) close() {
	self.file.Close()
}

func (self *checkPointFile) delete() {
	glog.V(2).Info("DELETE CHECKPOINT FILE %s", self.filePath)
	os.Remove(self.filePath)
}

func (self *checkPointFile) sync() error {
	return self.file.Sync()
}

func (self *checkPointFile) append(ck *checkPoint) error {
	_, err := fmt.Fprintf(self.file, "%d.%d.%d.%d\n", ck.RequestNumStart, ck.RequestNumEnd, ck.FirstOffset, ck.LastOffset)
	if err != nil {
		glog.Errorf("APPEND CHECKPOINT: %s", err.Error())
		return err
	}
	self.checkPoints = append(self.checkPoints, ck)
	return nil
}

func (self *checkPointFile) getLastOffset() int64 {
	if len(self.checkPoints) == 0 {
		return 0
	}
	return self.checkPoints[len(self.checkPoints)-1].LastOffset
}

func (self *checkPointFile) getLastRequestNum() uint32 {
	if len(self.checkPoints) == 0 {
		return 0
	}
	return self.checkPoints[len(self.checkPoints)-1].RequestNumEnd
}

func (self *checkPointFile) getRequestOffset(requestNum uint32) int64 {
	n := len(self.checkPoints)
	if n == 0 {
		return -1
	}
	if self.checkPoints[0].RequestNumStart > requestNum {
		return -1
	}
	if self.checkPoints[n-1].RequestNumEnd < requestNum {
		return -1
	}
	index := sort.Search(n, func(i int) bool {
		return requestNum <= self.checkPoints[i].RequestNumEnd
	})
	glog.Errorf("rn : %d, file %s offset %d", requestNum, self.file.Name(), self.checkPoints[index].FirstOffset)
	return self.checkPoints[index].FirstOffset
}
