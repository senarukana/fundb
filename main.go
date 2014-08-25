package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/senarukana/fundb/core"
	// "gitlab.baidu.com/go/glog"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())
	flag.Parse()

	handler, err := core.NewQueryEngine("leveldb", "data")
	if err != nil {
		log.Fatalln(err)
	}
	httpServer := core.NewHttpServer(":8080", handler)
	httpServer.ListenAndServe()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	for {
		_ = <-ch
		httpServer.Close()
		// glog.Infof("Got signal: %v", s)
		return
	}
}
