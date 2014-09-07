package configd

import (
	"io"
	"net"

	"github.com/senarukana/fundb/util"

	"github.com/golang/glog"
)

type tcpServer struct {
	configServer *ConfigServer
}

func (self *tcpServer) Handle(clientConn net.Conn) {
	glog.V(2).Infof("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		glog.Errorf("ERROR: failed to read protocol version - %s", err.Error())
		return
	}
	protocolMagic := string(buf)

	glog.V(2).Infof("CLIENT(%s): desired protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic)

	var prot util.Protocol
	switch protocolMagic {
	case "  V1":
		prot = &ConfigdProtocolV1{configServer: self.configServer}
	default:
		util.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		glog.Errorf("client(%s) bad protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		if err == io.EOF {
			glog.V(4).Infof("client(%s) is leaving", clientConn.RemoteAddr())
		} else {
			glog.Errorf("client(%s) - %s", clientConn.RemoteAddr(), err.Error())
		}
	}
}
