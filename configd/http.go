package configd

import (
	"io"
	"io/ioutil"
	"net/http"

	"github.com/senarukana/fundb/protocol"
	"github.com/senarukana/fundb/util"

	"code.google.com/p/goprotobuf/proto"
	"github.com/golang/glog"
)

type httpServer struct {
	configServer *ConfigServer
}

func (self *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/ping":
		self.pingHandler(w, req)
	case "/dbs":
		self.dbsHandler(w, req)
	case "/tables":
		self.tablesHandler(w, req)
	case "/create_shard":
		self.createShardHandler(w, req)
	case "/create_db":
		self.createDBHandler(w, req)
	case "/create_table":
		self.createTableHandler(w, req)
	default:
		util.ConfigdResponse(w, 404, "NOT_FOUND", nil)
	}
}

func (self *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (self *httpServer) createDBHandler(w http.ResponseWriter, req *http.Request) {
	dbName := req.URL.Query().Get("db")
	if dbName == "" {
		util.ConfigdResponse(w, 500, "MISSING_ARG_DB", nil)
		return
	}
	glog.V(1).Infof("CREATE DB %s", dbName)
	err := self.configServer.db.CreateDB(dbName)
	if err != nil {
		util.ConfigdResponse(w, 500, err.Error(), nil)
		return
	}
	util.ConfigdResponse(w, 200, "OK", nil)
}

func (self *httpServer) createTableHandler(w http.ResponseWriter, req *http.Request) {
	dbName := req.URL.Query().Get("db")
	if dbName == "" {
		util.ConfigdResponse(w, 500, "MISSING_ARG_DB", nil)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	table := &protocol.Table{}
	if err = proto.Unmarshal(body, table); err != nil {
		util.ConfigdResponse(w, 500, "INVALID_SHARD_INFO", nil)
		return
	}
	glog.V(1).Infof("CREATE Table %s", table)
	err = self.configServer.db.CreateTable(dbName, table)
	if err != nil {
		util.ConfigdResponse(w, 500, err.Error(), nil)
		return
	}
	util.ConfigdResponse(w, 200, "OK", nil)
}

func (self *httpServer) createShardHandler(w http.ResponseWriter, req *http.Request) {
	dbName := req.URL.Query().Get("db")
	if dbName == "" {
		util.ConfigdResponse(w, 500, "MISSING_ARG_DB", nil)
		return
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	shard := &protocol.Shard{}
	if err := proto.Unmarshal(body, shard); err != nil {
		util.ConfigdResponse(w, 500, "INVALID_SHARD_INFO", nil)
		return
	}
	glog.V(1).Infof("CREATE Shard %s", shard)
	err = self.configServer.db.CreateShard(dbName, shard)
	if err != nil {
		util.ConfigdResponse(w, 500, err.Error(), nil)
		return
	}
	util.ConfigdResponse(w, 200, "OK", nil)
}

func (self *httpServer) dbsHandler(w http.ResponseWriter, req *http.Request) {
	dbs := self.configServer.db.ListDB()
	data := make(map[string]interface{})
	data["data"] = dbs
	util.ConfigdResponse(w, 200, "OK", data)
}

func (self *httpServer) tablesHandler(w http.ResponseWriter, req *http.Request) {
	dbName := req.URL.Query().Get("db")
	if dbName == "" {
		util.ConfigdResponse(w, 500, "MISSING_ARG_DB", nil)
		return
	}
	tables, err := self.configServer.db.ListTables(dbName)
	if err != nil {
		util.ConfigdResponse(w, 500, err.Error(), nil)
		return
	}
	data := make(map[string]interface{})
	data["data"] = tables
	util.ConfigdResponse(w, 200, "OK", data)
}
