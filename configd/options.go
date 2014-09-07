package configd

import (
	"os"
	"time"

	"github.com/golang/glog"
)

type configServerOptions struct {
	Verbose bool `flag:"verbose"`

	TCPAddress       string        `flag:"tcp-address"`
	HTTPAddress      string        `flag:"http-address"`
	BroadcastAddress string        `flag:"broadcast-address"`
	InActiveTimeout  time.Duration `flag:"inactive-timeout"`
}

func NewConfigServerOptions(tcpAddr, httpAddr string) *configServerOptions {
	hostname, err := os.Hostname()
	if err != nil {
		glog.Fatal(err)
	}

	return &configServerOptions{
		TCPAddress:       tcpAddr,
		HTTPAddress:      httpAddr,
		BroadcastAddress: hostname,
		InActiveTimeout:  time.Second * 30,
	}
}
