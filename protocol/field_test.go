package protocol

import (
	"testing"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/bmizerany/assert"
)

func TestCanMarshalAndUnmarshal(t *testing.T) {
	field := &FieldValue{}

	f := int64(123)
	field.IntVal = &f

	record := &Record{}
	record.Values = []*FieldValue{field}
	ts := time.Now().Unix()
	record.Timestamp = &ts
	s := uint32(1)
	record.Serverid = &s

	d, err := proto.Marshal(record)
	assert.Equal(t, err, nil)

	unmarshalRecord := &Record{}
	err = proto.Unmarshal(d, unmarshalRecord)
	assert.Equal(t, err, nil)
	assert.Equal(t, unmarshalRecord.Values[0].GetIntVal(), f)
}
