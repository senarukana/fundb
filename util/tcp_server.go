package util

import (
	"net"
	"runtime"
	"strings"

	"github.com/golang/glog"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler) {
	glog.V(1).Infof("TCP: listening on %s", listener.Addr().String())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				glog.Errorf("NOTICE: temporary Accept() failure - %s", err.Error())
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				glog.Errorf("ERROR: listener.Accept() - %s", err.Error())
			}
			break
		}
		go handler.Handle(clientConn)
	}

	glog.V(1).Infof("TCP: closing %s", listener.Addr().String())
}
