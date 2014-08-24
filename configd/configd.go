package configd

import (
	"net"
	"os"

	"github.com/senarukana/fundb/util"

	"github.com/golang/glog"
)

type ConfigServer struct {
	id           uint32
	hostName     string
	options      *configServerOptions
	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	tcpListener  net.Listener
	httpListener net.Listener
	db           *db
	waitGroup    util.WaitGroupWrapper
}

func NewConfigServer(options *configServerOptions) *ConfigServer {
	hostname, err := os.Hostname()
	if err != nil {
		glog.Fatal(err)
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", options.TCPAddress)
	if err != nil {
		glog.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", options.HTTPAddress)
	if err != nil {
		glog.Fatal(err)
	}

	return &ConfigServer{
		options:  options,
		hostName: hostname,
		tcpAddr:  tcpAddr,
		httpAddr: httpAddr,
		db:       newDB(),
	}
}

func (self *ConfigServer) Start() {
	httpListener, err := net.Listen("tcp", self.httpAddr.String())
	if err != nil {
		glog.Fatalf("FATAL: listen (%s) failed - %s", self.httpAddr, err.Error())
	}
	self.httpListener = httpListener
	httpServer := &httpServer{configServer: self}

	self.waitGroup.Wrap(func() { util.HTTPServer(httpListener, httpServer, "HTTP") })
}

func (self *ConfigServer) Close() {
	if self.httpListener != nil {
		self.httpListener.Close()
	}
	self.waitGroup.Wait()
}
