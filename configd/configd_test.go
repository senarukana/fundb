package configd

import (
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/senarukana/fundb/meta"
	"github.com/senarukana/fundb/protocol"
	util "github.com/senarukana/fundb/util/configd"

	"code.google.com/p/goprotobuf/proto"
	"github.com/bmizerany/assert"
)

var (
	httpAddr = "0.0.0.0:9061"
	tcpAddr  = "0.0.0.0:9060"
)

func initConfigdServer() {
	opts := NewConfigServerOptions(tcpAddr, httpAddr)
	daemon := NewConfigServer(opts)
	daemon.Start()
}

func mustConnectLookupd(t *testing.T) net.Conn {
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		t.Fatal("failed to connect to lookupd")
	}
	conn.Write(util.MagicV1)
	return conn
}

func identify(t *testing.T, conn net.Conn) {
	ni := &protocol.NodeInfo{}
	ni.Id = proto.Uint32(1)
	ni.Address = proto.String("ip_address")
	ni.HostName = proto.String("test")
	ni.HttpPort = proto.Int32(9060)
	ni.TcpPort = proto.Int32(9061)
	bytes, err := proto.Marshal(ni)
	assert.Equal(t, err, nil)

	cmd := util.Identify(bytes)
	err = cmd.Write(conn)
	assert.Equal(t, err, nil)
	_, err = util.ReadResponse(conn)
	assert.Equal(t, err, nil)
}

func TestTcp(t *testing.T) {
	go initConfigdServer()
	time.Sleep(time.Microsecond * 10)

	nodes, err := util.GetNodesInfo(httpAddr)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(nodes), 0)

	conn := mustConnectLookupd(t)
	identify(t, conn)

	nodes, err = util.GetNodesInfo(httpAddr)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(nodes), 1)
}

func TestHttp(t *testing.T) {
	// go initConfigdServer()
	time.Sleep(time.Microsecond * 10)
	// test db
	for i := 0; i < 5; i++ {
		dbName := fmt.Sprintf("db%d", i)
		endpoint := fmt.Sprintf("http://%s/create_db?db=%s", httpAddr, dbName)
		resp, err := util.ConfigdRequest(endpoint)
		assert.Equal(t, err, nil)
		assert.Equal(t, resp.MustString(), "")
	}

	endpoint := fmt.Sprintf("http://%s/dbs", httpAddr)
	resp, err := util.ConfigdRequest(endpoint)
	assert.Equal(t, err, nil)
	dbs, err := resp.Get("dbs").StringArray()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(dbs), 5)

	// test table
	dbName := "db0"
	for i := 0; i < 5; i++ {
		tbName := fmt.Sprintf("table%d", i)
		tb := meta.NewTable(tbName, tbName, tbName)
		endpoint := fmt.Sprintf("http://%s/create_table?db=%s", httpAddr, dbName)
		resp, err := util.ConfigdPostRequest(endpoint, tb)
		assert.Equal(t, err, nil)
		assert.Equal(t, resp.MustString(), "")
	}

	endpoint = fmt.Sprintf("http://%s/tables?db=%s", httpAddr, dbName)
	resp, err = util.ConfigdRequest(endpoint)
	assert.Equal(t, err, nil)
	tables, err := resp.Get("tables").StringArray()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(tables), 5)

	sort.Strings(tables)
	for i, name := range tables {
		assert.Equal(t, name, fmt.Sprintf("table%d", i))
	}

	// test meta
	endpoint = fmt.Sprintf("http://%s/meta", httpAddr)
	resp, err = util.ConfigdRequest(endpoint)
	assert.Equal(t, err, nil)
	version := resp.Get("version").MustInt()
	assert.Equal(t, err, nil)
	assert.Equal(t, version, 0)
	bytes, err := resp.Get("dbs").Bytes()
	assert.Equal(t, err, nil)
	metaData, err := meta.Recovery(bytes)
	assert.Equal(t, err, nil)

	assert.Equal(t, len(metaData.ListDB()), 5)
}
