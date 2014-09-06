package wal

import (
	"testing"
	// "time"

	"github.com/senarukana/fundb/protocol"

	"github.com/bmizerany/assert"
)

func generateRequest() *protocol.Request {
	query := "INSERT INTO T VALUES (1, 2)"
	return &protocol.Request{
		Query: &query,
	}
}

var recoverCounter = 0

func recoverDoCounter(request *protocol.Request) error {
	recoverCounter++
	return nil
}

func TestWALAppendAndCommit(t *testing.T) {
	wal, err := NewWriteAheadLog()
	if err != nil {
		t.Fatalf("OPEN WAL ERROR: %s", err.Error())
	}

	for i := 0; i < 1000; i++ {
		reqNum, err := wal.Append(generateRequest())
		assert.Equal(t, reqNum, uint32(i+1))
		if err != nil {
			t.Fatal(err.Error())
		}
		assert.Equal(t, err, nil)
	}

	for i := 0; i < 1000; i++ {
		wal.Commit(uint32(i + 1))
	}
	wal.Close(true)
	wal.truncate()
}

func TestWALRecovery(t *testing.T) {
	wal, err := NewWriteAheadLog()
	assert.Equal(t, err, nil)

	appendNum := 1000
	commitNum := 500

	for i := 0; i < appendNum; i++ {
		reqNum, err := wal.Append(generateRequest())
		assert.Equal(t, reqNum, uint32(i+1))
		assert.Equal(t, err, nil)
	}

	for i := 0; i < commitNum; i++ {
		wal.Commit(uint32(i + 1))
	}
	wal.Close(true)

	wal, err = NewWriteAheadLog()
	assert.Equal(t, err, nil)
	assert.Equal(t, wal.state.CurrentRequestNum, uint32(appendNum))

	recoverCounter = 0
	err = wal.RecoverFromLastCommit(recoverDoCounter)
	wal.truncate()
	assert.Equal(t, err, nil)
	assert.Equal(t, recoverCounter, appendNum-commitNum+1)
}
