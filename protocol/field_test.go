package protocol

import (
	"testing"

	"code.google.com/p/goprotobuf/proto"
	"github.com/bmizerany/assert"
)

func TestCanMarshalAndUnmarshal(t *testing.T) {
	field := &FieldValue{}

	f := int64(123)
	field.IntVal = &f

	record := &Record{}
	record.Values = []*FieldValue{field}
	id := int64(1)
	record.Id = &id
	s := uint32(1)
	record.SequenceNum = &s

	d, err := proto.Marshal(record)
	assert.Equal(t, err, nil)

	unmarshalRecord := &Record{}
	err = proto.Unmarshal(d, unmarshalRecord)
	assert.Equal(t, err, nil)
	assert.Equal(t, unmarshalRecord.Values[0].GetIntVal(), f)

	oid := unmarshalRecord.GetId()
	*unmarshalRecord.Id += 5
	assert.Equal(t, unmarshalRecord.GetId(), oid+5)
}
