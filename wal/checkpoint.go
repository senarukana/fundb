package wal

type checkPoint struct {
	LogFileSuffix   int
	RequestNumStart uint64
	RequestNumEnd   uint64
}

type checkPointFile struct {
	checkPoints   []*checkPoint
	logFileSuffix int
	dir           string
}

func newCheckPointManager(dir string) *checkPointManager {

}
