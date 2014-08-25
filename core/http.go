package core

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"

	"github.com/bmizerany/pat"
	"github.com/golang/glog"
)

type HttpServer struct {
	conn     net.Listener
	addr     string
	handler  *QueryEngine
	exitChan chan int
}

func NewHttpServer(addr string, handler *QueryEngine) *HttpServer {
	return &HttpServer{
		addr:     addr,
		handler:  handler,
		exitChan: make(chan int),
	}
}

func headerHandler(handler http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Add("Access-Control-Allow-Origin", "*")
		rw.Header().Add("Access-Control-Max-Age", "2592000")
		rw.Header().Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		rw.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		handler(rw, req)
	}
}

func (self *HttpServer) ListenAndServe() {
	conn, err := net.Listen("tcp", self.addr)
	if err != nil {
		glog.Fatalf("Listen error: %s", err)
	}
	self.conn = conn
	self.Serve()
}

func (self *HttpServer) Serve() {
	p := pat.New()

	p.Get("/db/:db/query", headerHandler(self.query))
	p.Post("/db", headerHandler(self.createDatabase))
	p.Get("/db", headerHandler(self.listDatabase))
	if err := http.Serve(self.conn, p); err != nil && strings.Contains(err.Error(), "closed network") {
		panic(err)
	}
	close(self.exitChan)
}

func (self *HttpServer) Close() {
	glog.V(1).Infof("CLOSING: Http server")
	self.conn.Close()
	<-self.exitChan
}

func (self *HttpServer) query(writer http.ResponseWriter, request *http.Request) {
	db := request.URL.Query().Get(":db")
	query := request.URL.Query().Get("q")

	response := self.handler.Query(db, query)
	self.write(writer, response)
}

func (self *HttpServer) createDatabase(writer http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

}

func (self *HttpServer) listDatabase(writer http.ResponseWriter, req *http.Request) {

}

func (self *HttpServer) write(writer http.ResponseWriter, response *Response) {
	data, err := json.Marshal(response)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
		return
	}
	writer.Header().Add("content-type", "application/json")
	writer.WriteHeader(http.StatusOK)
	writer.Write(data)
}
