package wal

import (
	"os"
)

type logFile struct {
	*os.File
	offset int
	suffix int
	path   string
}

func newLogFile(suffix int) {

}
