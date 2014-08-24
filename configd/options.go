package configd

import (
	"log"
	"os"
)

type configServerOptions struct {
	Verbose bool `flag:"verbose"`

	TCPAddress       string `flag:"tcp-address"`
	HTTPAddress      string `flag:"http-address"`
	BroadcastAddress string `flag:"broadcast-address"`
}

func NewConfigServerOptions() *configServerOptions {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &configServerOptions{
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname,
	}
}
