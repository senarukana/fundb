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

type replayRequest struct {
	request *protocol.Request
	err     error
}

func newReplayRequest(request *protocol.Request) *replayRequest {
	return &replayRequest{
		request: request,
	}
}
