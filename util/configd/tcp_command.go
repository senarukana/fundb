package configd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var byteSpace = []byte(" ")
var byteNewLine = []byte("\n")

type Command struct {
	Name   []byte
	Params [][]byte
	Body   []byte
}

// String returns the name and parameters of the Command
func (c *Command) String() string {
	if len(c.Params) > 0 {
		return fmt.Sprintf("%s %s", c.Name, string(bytes.Join(c.Params, byteSpace)))
	}
	return string(c.Name)
}

func (self *Command) Write(w io.Writer) (err error) {
	if _, err := w.Write(self.Name); err != nil {
		return err
	}
	for _, param := range self.Params {
		if _, err = w.Write(byteSpace); err != nil {
			return err
		}
		if _, err = w.Write(param); err != nil {
			return err
		}
	}
	if _, err = w.Write(byteNewLine); err != nil {
		return err
	}
	if self.Body != nil {
		bodySize := int32(len(self.Body))
		if err = binary.Write(w, binary.BigEndian, bodySize); err != nil {
			return err
		}
		if _, err = w.Write(self.Body); err != nil {
			return err
		}
	}
	return nil
}

// Identify creates a new Command to provide information about the client.  After connecting,
// it is generally the first message sent.
func Identify(body []byte) *Command {
	return &Command{[]byte("IDENTIFY"), nil, body}
}

// Ping creates a new Command to keep-alive the state of all the
// announced topic/channels for a given client
func Ping() *Command {
	return &Command{[]byte("PING"), nil, nil}
}
