package wal

type state struct {
	Path           string
	NextRequestNum uint64
	CurrentFileNum int32
	CurrentFilePos int32
}
