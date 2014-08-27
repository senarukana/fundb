package wal

import (
	"github.com/senarukana/fundb/protocol"
)

type response struct {
	requestNum uint32
	err        error
}

type appendRequest struct {
	request  *protocol.Request
	respChan chan *response
}
