package engine

import (
	"fmt"
	"hash/fnv"
)

const (
	SEPERATOR = '|'
)

func getIdForDBTableColumn(db, series, column string) (ret []byte) {
	s := fmt.Sprintf("%s%c%s%c%s", db, SEPERATOR, series, SEPERATOR, column)
	hash := fnv.New64a()
	return hash.Sum([]byte(s))
}
