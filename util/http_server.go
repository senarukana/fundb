package util

import (
	"net"
	"net/http"
	"strings"

	"github.com/golang/glog"
)

func HTTPServer(listener net.Listener, handler http.Handler, proto_name string) {
	glog.V(2).Infof("%s: listening on %s", proto_name, listener.Addr().String())

	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		glog.V(2).Infof("ERROR: http.Serve() - %s", err.Error())
	}

	glog.V(2).Infof("%s: closing %s", proto_name, listener.Addr().String())
}
