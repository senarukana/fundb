package configd

import (
	"encoding/binary"
	"io"
)

// ReadResponse is a client-side utility function to read from the supplied Reader
// according to the NSQ protocol spec:
//
//    [x][x][x][x][x][x][x][x]...
//    |  (int32) || (binary)
//    |  4-byte  || N-byte
//    ------------------------...
//        size       data
func ReadResponse(rd io.Reader) ([]byte, error) {
	var responseSize int32
	err := binary.Read(rd, binary.BigEndian, &responseSize)
	if err != nil {
		return nil, err
	}
	response := make([]byte, responseSize)
	if _, err = io.ReadFull(rd, response); err != nil {
		return nil, err
	}
	return response, nil
}
