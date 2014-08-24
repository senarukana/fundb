package configd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/senarukana/fundb/protocol"
	"github.com/senarukana/fundb/util"

	"code.google.com/p/goprotobuf/proto"
	"github.com/golang/glog"
)

type ConfigdProtocolV1 struct {
	configServer *ConfigServer
}

type ClientV1 struct {
	net.Conn
	nodeInfo *protocol.NodeInfo
}

func NewClientV1(conn net.Conn) *ClientV1 {
	return &ClientV1{
		Conn: conn,
	}
}

func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}

func (self *ConfigdProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClientV1(conn)
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		response, err := self.Exec(client, reader, params)
		if err != nil {
			glog.Errorf("EXEC COMMAND [%s] ERROR: %s", params[0], err)
			_, err = util.SendResponse(client, []byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}
		if response != nil {
			_, err = util.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}
	glog.V(2).Infof("CLIENT(%s): closing", client)
	if client.nodeInfo != nil {
		self.configServer.db.RemoveNode(client.nodeInfo)
		glog.V(2).Infof("client(%s) UNREGISTER", client)
	}
	return err
}

func (self *ConfigdProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return self.Ping(client, params)
	case "IDENTIFY":
		return self.Identify(client, reader, params[1:])
	}
	return nil, fmt.Errorf("UNKNOWN COMMAND %s", params[0])
}

func (self *ConfigdProtocolV1) Identify(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.nodeInfo != nil {
		return nil, fmt.Errorf("cannot IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	nodeInfo := protocol.NodeInfo{}
	err = proto.Unmarshal(body, &nodeInfo)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to decode node info")
	}
	addr := client.RemoteAddr().String()
	nanoTime := time.Now().UnixNano()
	nodeInfo.Address = &addr
	nodeInfo.LastUpdate = &nanoTime

	glog.V(1).Infof("CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d",
		client, nodeInfo.GetAddress(), nodeInfo.GetTcpPort(), nodeInfo.GetHttpPort())

	client.nodeInfo = &nodeInfo
	if self.configServer.db.AddNode(&nodeInfo) {
		glog.V(2).Infof("DB: client(%s) REGISTER COMPLETE", client)
	}

	tcpPort := int32(self.configServer.tcpAddr.Port)
	httpPort := int32(self.configServer.tcpAddr.Port)
	// build a response
	responseInfo := &protocol.NodeInfo{
		Id:       &self.configServer.id,
		Address:  &self.configServer.options.BroadcastAddress,
		HostName: &self.configServer.hostName,
		TcpPort:  &tcpPort,
		HttpPort: &httpPort,
	}

	response, err := proto.Marshal(responseInfo)
	if err != nil {
		glog.Errorf("marshaling response info %v", responseInfo)
		return []byte("OK"), nil
	}
	return response, nil
}

func (self *ConfigdProtocolV1) Ping(client *ClientV1, params []string) ([]byte, error) {
	if client.nodeInfo != nil {
		now := time.Now().UnixNano()
		glog.V(3).Infof("CLIENT(%s): pinged (last ping %d ms)", client.nodeInfo.GetAddress(), (now-client.nodeInfo.GetLastUpdate())/1000)
		client.nodeInfo.LastUpdate = &now
	}
	return []byte("OK"), nil
}
