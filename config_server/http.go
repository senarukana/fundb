package config_server

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
	case "/shards":
		self.shardsHandler(w, req)
	case "/dbs":
		self.dbsHandler(w, req)
	case "/create_shard":
		self.createShardHandler(w, req)
	case "/create_db":
		self.createDBHandler(w, req)
	default:
		util.ApiResponse(w, 404, "NOT_FOUND", nil)
	}
}

func (self *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (self *httpServer) createDBHandler(w http.ResponseWriter, req *http.Request) {
	dbName := req.URL.Query().Get("db")
	if dbName == "" {
		util.ApiResponse(w, 500, "MISSING_ARG_DB", nil)
		return
	}
	glog.V(1).Infof("CREATE DB %s", dbName)
	err := self.configServer.db.createDB(dbName)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
		return
	}
	util.ApiResponse(w, 200, "OK", nil)
}

func (self *httpServer) createShardHandler(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	shard := &protocol.Shard{}
	if err := proto.Unmarshal(body, shard); err != nil {
		util.ApiResponse(w, 500, "INVALID_SHARD_INFO", nil)
		return
	}
	glog.V(1).Infof("CREATE Shard %s", shard)
	self.configServer.db.createShard(shard)
	util.ApiResponse(w, 200, "OK", nil)
}

func (self *httpServer) dbsHandler(w http.ResponseWriter, req *http.Request) {
	dbs := self.configServer.db.listDB()
	data := make(map[string]interface{})
	data["dbs"] = dbs
	util.ApiResponse(w, 200, "OK", data)
}

func (self *httpServer) shardsHandler(w http.ResponseWriter, req *http.Request) {
	shards := self.configServer.db.shards
	data := make(map[string]interface{})
	data["shards"] = shards
	util.ApiResponse(w, 200, "OK", data)
}
