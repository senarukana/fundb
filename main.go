package main

import (
	"log"

	"github.com/senarukana/fundb/core"
)

func main() {
	query := "INSERT INTO T (id, name, age) VALUES (1, 'li', 25), (2, 'ted', 25)"
	engine, err := core.NewEngineHandler("leveldb", "data")
	if err != nil {
		log.Fatalln(err)
	}
	response := engine.Query(query)
	if response.Error != nil {
		log.Fatalf("Query Error:%s\n", response.Error)
	}
	log.Println(response.RowsAffected)
}
